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
 * <p><b>A note about threading:</b> This system is designed to have one
 * principal thread.  This thread will execute the asyncMain function,
 * and the various callbacks will be called from it.  If you want to run
 * another thread, you can pass events to it through any synchronized
 * queue.  To pass events back to the main thread, call registerTimer
 * with a delay of 0.  Both registerTimer and cancelTimer are properly
 * synchronized for this purpose.
 *
 * @author Sean C. Rhea
 * @version $Id: ASyncCore.java,v 1.30 2005/04/27 20:51:10 srhea Exp $
 */
public abstract class ASyncCore {

    //////////////////////////////////////////////////////////////////
    //                        Current Interface                     //
    //  These functions comprise the latest interface.  The other   //
    //  functions below them are for backward compatibility only.   //
    //////////////////////////////////////////////////////////////////

    public abstract void registerSelectable (
	    SelectableChannel channel, int interestOps, Runnable callback) 
            throws ClosedChannelException;

    public abstract void unregisterSelectable (
	    SelectableChannel channel, int interestOps) 
            throws ClosedChannelException;

    public abstract void unregisterSelectable (SelectableChannel channel);

    public abstract long nowMillis ();
    
    public abstract long timerMillis();

    public abstract void asyncMain();

    /**
     * Register a function to be called sometime later than
     * <code>delayMillis</code> milliseconds from now.  This function is safe
     * to call from any thread.  The return value may be used as an argument
     * to cancelTimer, but is otherwise opaque to the user.
     */
    public abstract Object registerTimer(long delayMillis, Runnable cb);

    public abstract void cancelTimer (Object token);

    //////////////////////////////////////////////////////////////////
    //                         Older Interface                      //
    //////////////////////////////////////////////////////////////////

    public static interface SelectableCB {
	void select_cb (SelectionKey key, Object user_data);
    }

    public static interface TimerCB {
	void timer_cb (Object user_data);
    }

    public abstract SelectionKey register_selectable (
	    SelectableChannel channel, int interest_ops,
	    SelectableCB cb, Object user_data) throws ClosedChannelException;

    public abstract void unregister_selectable (SelectionKey skey);

    //////////////////////////////////////////////////////////////////
    //            These functions are also part of the older        //
    //           interface, but they can be implemented using       //
    //              the new one, so they're not abstract.           //
    //////////////////////////////////////////////////////////////////

    public Object register_timer (
	    long time_ms, final TimerCB cb, final Object user_data) {
        return registerTimer(time_ms, new Runnable() { 
                public void run() { cb.timer_cb(user_data); }});
    }

    public Object register_timer(long delayMillis, Runnable cb) {
        return registerTimer(delayMillis, cb);
    }

    public void register_selectable (
	    SelectableChannel channel, int interestOps, Runnable callback) 
            throws ClosedChannelException {
        registerSelectable(channel, interestOps, callback);
    }

    public void unregister_selectable (
	    SelectableChannel channel, int interestOps) 
            throws ClosedChannelException {
        unregisterSelectable(channel, interestOps);
    }

    public void unregister_selectable (SelectableChannel channel) {
        unregisterSelectable(channel);
    }

    public void cancel_timer (Object token) { cancelTimer(token); }

    public void async_main () { asyncMain(); }
}

