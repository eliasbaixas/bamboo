/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.lss;
import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.channels.SelectableChannel;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.helpers.NullEnumeration;
import static bamboo.util.Curry.*;
import static java.nio.channels.SelectionKey.*;
import bamboo.util.PriorityQueue;
import bamboo.util.GetTimeOfDayCC;

/**
 * A main-loop largely inspired by David Mazieres' libasync, but using
 * java.nio.
 *
 * <b>A note about threading:</b> This system is designed to have one
 * principal thread.  This thread will execute the main function, and the
 * various callbacks (SelectableCBs and TimerCBs) will be called from it.  If
 * you want to run another thread, you can pass events to it through any
 * synchronized queue.  To pass events back to the main thread, call
 * register_timer with a time of 0.  Both register_timer and cancel_timer are
 * properly synchronized for this purpose.
 *
 * @author Sean C. Rhea
 * @version $Id: ASyncCoreImpl.java,v 1.2 2005/06/04 18:08:29 srhea Exp $
 */
public class ASyncCoreImpl extends ASyncCore {

    protected Logger logger;

    public ASyncCoreImpl () throws IOException {
	selector = Selector.open ();
        if (Logger.getRoot ().getAllAppenders () instanceof NullEnumeration) {
            PatternLayout pl = new PatternLayout ("%c: %m\n");
            ConsoleAppender ca = new ConsoleAppender (pl);
            Logger.getRoot ().addAppender (ca);
            Logger.getRoot ().setLevel (Level.INFO);
        }
        logger = Logger.getLogger (getClass ());
    }

    public SelectionKey register_selectable (
	    SelectableChannel channel, int interest_ops,
	    SelectableCB cb, Object user_data) throws ClosedChannelException {

	if (logger.isDebugEnabled ())
            logger.debug ("ASyncCore.register_selectable");
	SelectionKey skey = channel.register (selector, interest_ops);
	selectable_info.put (channel, new SelectableInfo (cb, user_data));
	return skey;
    }

    public void unregister_selectable (SelectionKey skey) {
	selectable_info.remove (skey.channel ());
        skey.cancel ();
    }

    public void registerSelectable (
	    SelectableChannel channel, int interest_ops, Runnable cb) 
            throws ClosedChannelException {

	if (logger.isDebugEnabled ())
            logger.debug ("ASyncCore.register_selectable");
	SelectionKey skey = channel.keyFor (selector);
        if (skey == null)
            skey = channel.register (selector, interest_ops);
        else
            skey.interestOps (skey.interestOps () | interest_ops);
        SelectableInfo si = (SelectableInfo) selectable_info.get (channel);
        if (si == null) {
            si = new SelectableInfo ();
            selectable_info.put (channel, si);
        }
        if ((interest_ops & SelectionKey.OP_ACCEPT) != 0)
            si.accept_cb = cb;
        if ((interest_ops & SelectionKey.OP_CONNECT) != 0)
            si.connect_cb = cb;
        if ((interest_ops & SelectionKey.OP_READ) != 0)
            si.read_cb = cb;
        if ((interest_ops & SelectionKey.OP_WRITE) != 0)
            si.write_cb = cb;
    }

    public void unregisterSelectable (
	    SelectableChannel channel, int interest_ops) 
        throws ClosedChannelException {

	if (logger.isDebugEnabled ())
            logger.debug ("ASyncCore.register_selectable");
	SelectionKey skey = channel.keyFor (selector);
        skey.interestOps (skey.interestOps () & ~interest_ops);
        SelectableInfo si = (SelectableInfo) selectable_info.get (channel);
        if ((interest_ops & SelectionKey.OP_ACCEPT) != 0)
            si.accept_cb = null;
        if ((interest_ops & SelectionKey.OP_CONNECT) != 0)
            si.connect_cb = null;
        if ((interest_ops & SelectionKey.OP_READ) != 0)
            si.read_cb = null;
        if ((interest_ops & SelectionKey.OP_WRITE) != 0)
            si.write_cb = null;
        if (skey.interestOps () == 0)
            selectable_info.remove (channel);
    }

    public void unregisterSelectable (SelectableChannel channel) {
	SelectionKey skey = channel.keyFor (selector);
        if (skey.isValid ())
            skey.interestOps (0);
        selectable_info.remove (channel);
    }

    public long nowMillis() {
        return System.currentTimeMillis();
    }

    public long timerMillis() {
        if (GetTimeOfDayCC.available ())
            return GetTimeOfDayCC.currentTimeMillis ();
        return nowMillis();
    }

    public Object registerTimer(long time_ms, Runnable cb) {
        return register_timer_impl(time_ms, cb, null);
    }

    private Object register_timer_impl (
	    long time_ms, Object cb, Object user_data) {

	if (logger.isDebugEnabled ()) logger.debug ("ASyncCore.register_timer");
	TimerInfo result = new TimerInfo (cb, user_data);

        boolean wakeup = false;
        synchronized (timers) {
            long now_ms = nowMillis();
            long this_timer = time_ms + now_ms;
            if (timers.isEmpty ())
                wakeup = true;
            else {
                long first_timer = timers.getFirstPriority ().longValue ();
                if (this_timer < first_timer)
                    wakeup = true;
            }

            timers.add (result, new Long(this_timer));
        }

        if (wakeup)
            selector.wakeup ();

	if (logger.isDebugEnabled ()) logger.debug ("set timer time_ms=" +
		time_ms + " ti=" + result);
	return result;
    }

    public void cancelTimer (Object token) {
	if (! (token instanceof TimerInfo))
	    throw new IllegalArgumentException ();
        synchronized (timers) {
            cancelled_timers.add ((TimerInfo) token);
        }
    }

    /** 
     * Set this flag to false before calling asyncMain to disable the watchdog.
     */
    public boolean useWatchdog = true;

    protected boolean watchdogFlag = true;
    protected long watchdogCheckPeriod = 10*1000;
    protected long watchdogResetPeriod = 3*1000;
    protected Object watchdogLock = new Object();

    protected class Watchdog extends Thread {
        public void run() {
            while(true) {
                long now = System.currentTimeMillis();
                long wake = now + watchdogCheckPeriod;
                do {
                    try { Thread.sleep(wake - now); } 
                    catch (InterruptedException e) {}
                    now = System.currentTimeMillis();
                }
                while (now < wake);
                boolean flag = false;
                synchronized(watchdogLock) {
                    flag = watchdogFlag;
                    watchdogFlag = true;
                }
                if (flag)
                    logger.warn("main thread appears stalled");
                else
                    logger.info("watchdog awakened");
            }
        }
    }

    protected class WatchdogReset implements Runnable {
        public void run() {
            synchronized(watchdogLock) {
                watchdogFlag = false;
            }
            registerTimer(watchdogResetPeriod, this);
        }
    }

    public void asyncMain () {

        if (logger.isDebugEnabled ()) logger.debug ("async_main called");

        if (useWatchdog) {
            registerTimer(0, new WatchdogReset());
            (new Watchdog()).start();
        }

	while (true) {

	    // Get rid of any cancelled timers.

            synchronized (timers) {
                while (! timers.isEmpty ()) {
                    TimerInfo ti = timers.getFirst ();
                    if (cancelled_timers.remove (ti)) {
                        if (logger.isDebugEnabled ())
                            logger.debug ("removing timer " + ti);
                        timers.removeFirst ();
                    }
                    else {
                        if (logger.isDebugEnabled ())
                            logger.debug("finished removing timers");
                        break;
                    }
                }
            }

	    // Select until the next timer, or indefinitely.

	    long now_ms = nowMillis();
	    if (logger.isDebugEnabled ()) logger.debug ("now_ms=" + now_ms);
	    // if (logger.isDebugEnabled ()) logger.debug ("timers=" + timers);

	    try {
                // I can't hold a lock on timers while selecting, or no other
                // thread will be able to wake me up in register_timer.
                // Instead, I just get the head of the timers queue and then
                // call select.  If someone calls register_timer between when
                // I check the head of the queue and when I call select, and
                // that new timer preceeds the previous head of the queue,
                // they'll call wakeup, which will cause the next call to
                // select to wake up immediately, so no deadlock is possible
                // here.

                long time_ms = -1;
                synchronized (timers) {
                    if (! timers.isEmpty ())
                        time_ms = timers.getFirstPriority ().longValue ();
                }

                if (time_ms == -1) {
                    // block indefinitely
                    if (logger.isDebugEnabled ())
                        logger.debug ("calling select ()");
                    selector.select ();
                }
                else {
                    if (now_ms >= time_ms) {
                        // do not block
                        if (logger.isDebugEnabled ()) logger.debug (
                                "calling selectNow ()");
                        selector.selectNow ();
                    }
                    else {
                        // block until next timer expires
                        if (logger.isDebugEnabled ())
                            logger.debug ("calling select (" +
                                (time_ms - now_ms) + ")");
                        selector.select (time_ms - now_ms);
                    }
                }
            }
	    catch (IOException e) {
                if (! e.getMessage ().equals ("Interrupted system call")) {
                    logger.fatal ("unhandled exception: " + e, e);
                    System.exit (1);
                }
	    }

            if (logger.isDebugEnabled ()) logger.debug ("select returned");

	    // Process the ready selectables.

	    Iterator i = selector.selectedKeys ().iterator ();
	    while (i.hasNext ()) {
		SelectionKey skey = (SelectionKey) i.next ();
                i.remove ();
		SelectableInfo si = selectable_info.get (skey.channel ());
                if (si == null)
                    continue;
                // Each of these callbacks could cancel this skey, so we need
                // to check that it's still valid after each one before
                // calling the next one.
                if (si.cb != null)
                    ((SelectableCB) si.cb).select_cb (skey, si.user_data);
                if (skey.isValid () && (si.accept_cb != null) && 
                    ((skey.readyOps () & OP_ACCEPT) != 0))
                    si.accept_cb.run();
                if (skey.isValid () && (si.connect_cb != null) && 
                    ((skey.readyOps () & OP_CONNECT) != 0))
                    si.connect_cb.run();
                if (skey.isValid () && (si.read_cb != null) && 
                    ((skey.readyOps () & OP_READ) != 0))
                    si.read_cb.run();
                if (skey.isValid () && (si.write_cb != null) && 
                    ((skey.readyOps () & OP_WRITE) != 0))
                    si.write_cb.run();
            }

	    // Process the ready timers.

	    while (true) {
		TimerInfo info = null;
                synchronized (timers) {
                    if (timers.isEmpty ())
                        break;
                    long time_ms = timers.getFirstPriority ().longValue ();
                    if (now_ms < time_ms)
                        break;
                    info = timers.removeFirst ();
                    if (cancelled_timers.remove(info)) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Skipping cancelled timer " + info);
                        }
                        continue;
                    }
                }
                if (logger.isDebugEnabled ())
                    logger.debug ("calling timer " + info);
                // Don't hold the lock during the callback.
                if (info.cb instanceof TimerCB) 
                    ((TimerCB) info.cb).timer_cb (info.user_data);
                else
                    ((Runnable) info.cb).run();
	    }
	}
    }

    protected Selector selector;
    protected HashMap<SelectableChannel,SelectableInfo> selectable_info 
        = new HashMap<SelectableChannel,SelectableInfo> ();
    protected PriorityQueue<TimerInfo,Long> timers = 
        new PriorityQueue<TimerInfo,Long>(200);
    protected HashSet<TimerInfo> cancelled_timers = new HashSet<TimerInfo> ();

    protected static class SelectableInfo {
	public SelectableCB cb;
	public Object user_data;
        public Runnable accept_cb, connect_cb, read_cb, write_cb;
	public SelectableInfo () {}
	public SelectableInfo (SelectableCB c, Object u) {
	    cb = c; user_data = u;
	}
    }

    protected static class TimerInfo {
	public Object cb;
	public Object user_data;
	public TimerInfo (Object c, Object u) {
	    cb = c; user_data = u;
	}
	public String toString () {
	    return "(TimerInfo cb=" + cb + " user_data=" + user_data + ")";
	}
    }

    /**
     * Tests the watchdog.
     */ 
    public static void main(String args[]) throws Exception {
        PatternLayout pl = new PatternLayout ("%d{ISO8601} %-5p %c: %m\n");
        ConsoleAppender ca = new ConsoleAppender (pl);
        Logger.getRoot ().addAppender (ca);
        Logger.getRoot ().setLevel (Level.INFO);

        final ASyncCoreImpl acore = new ASyncCoreImpl();
        acore.registerTimer(acore.watchdogCheckPeriod * 3, new Runnable() { 
                public void run() { 
                    acore.logger.info("starting infinite loop"); 
                    while (true);
                }
            });
        acore.asyncMain();
    }
}

