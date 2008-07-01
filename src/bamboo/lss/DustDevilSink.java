/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.lss;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import seda.sandStorm.api.SinkIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.EventHandlerIF;
import seda.sandStorm.api.EnqueuePredicateIF;
import seda.sandStorm.api.SinkException;
import seda.sandStorm.api.SinkFullException;
import seda.sandStorm.api.EventHandlerException;

/**
 * Part of faking out Sandstorm.
 *
 * @author Sean C. Rhea
 * @version $Id: DustDevilSink.java,v 1.5 2004/07/15 19:14:57 srhea Exp $
 */
public class DustDevilSink implements SinkIF {

    public static byte [] reserve = new byte [256*1024];
    protected static Logger logger = Logger.getLogger ("DustDevilSink");
    protected static int next_hash_code = 0;

    protected final int hash_code;
    protected final Thread thread;
    protected final EventHandlerIF handler;

    protected Map prepares = new HashMap ();
    protected long next_key;

    /**
     * Create a new sink that sends all events to the given
     * <code>EventHandlerIF</code>; the thread <code>t</code> is a guard--the
     * functions of this class may not be called from any other thread.
     * This restriction prevents programming errors.  This constructor is not
     * thread safe; since these objects should only be created within the
     * <code>DustDevil</code>, that shouldn't be a problem.
     */
    public DustDevilSink (EventHandlerIF h, Thread t) {
        if (h == null) 
            throw new IllegalArgumentException ("h==null");
        if (t == null) 
            throw new IllegalArgumentException ("t==null");

        hash_code = next_hash_code++;
	handler = h;
        thread = t;
    }

    public int hashCode () {
	return hash_code;
    }

    public void enqueue (QueueElementIF event) throws SinkException {
        check_thread ();
	QueueElementIF [] events = {event};
	try {
	    handler.handleEvents (events);
	}
	catch (EventHandlerException e) {
	    throw new SinkFullException ("from EventHandlerException");
	}
        catch (OutOfMemoryError e) {
            reserve = null;
            System.gc ();
            logger.fatal("caught OutOfMemoryError on enqueue of " 
                    + event.getClass ().getName () + " to " 
                    + handler.getClass ().getName ());
            logger.fatal("event=" + event);
            logger.fatal("freeMemory=" + Runtime.getRuntime ().freeMemory ());
            logger.fatal("maxMemory=" + Runtime.getRuntime ().maxMemory ());
            logger.fatal("totalMemory=" + Runtime.getRuntime ().totalMemory ());
            Thread.currentThread ().dumpStack ();
            System.exit (1);
        }
    }

    public boolean enqueue_lossy (QueueElementIF event) {
        check_thread ();
	try {
	    enqueue (event);
	    return true;
	} catch (SinkException e) {
	    return false;
	}
    }

    public void enqueue_many (QueueElementIF [] events) throws SinkException {
        check_thread ();
	try {
	    handler.handleEvents (events);
	}
	catch (EventHandlerException e) {
            logger.fatal ("caught EventHandlerException", e);
	    System.exit (1);
	}
        catch (OutOfMemoryError e) {
            reserve = null;
            System.gc ();
            logger.fatal("caught OutOfMemoryError on enqueue_many to " 
                    + handler.getClass ().getName () + " events:");
            for (int i = 0; i < events.length; ++i) 
                logger.fatal(events[i].getClass().getName() + ": " + events[i]);
            logger.fatal("freeMemory=" + Runtime.getRuntime ().freeMemory ());
            logger.fatal("maxMemory=" + Runtime.getRuntime ().maxMemory ());
            logger.fatal("totalMemory=" + Runtime.getRuntime ().totalMemory ());
            Thread.currentThread ().dumpStack ();
            System.exit (1);
        }
    }

    public Object enqueue_prepare (QueueElementIF [] e) throws SinkException {
        check_thread ();
	Long key = new Long (next_key++);
	prepares.put (key, e);
	return key;
    }

    public void enqueue_commit (Object key) {
        check_thread ();
	QueueElementIF [] events = (QueueElementIF []) prepares.remove (key);
        if (events == null)
            throw new IllegalArgumentException (
                    "enqueue_commit called with unknown key: " + key);
	try {
	    enqueue_many (events);
	}
	catch (SinkException e) {
            logger.fatal ("caught SinkException", e);
	    System.exit (1);
	}
    }

    public void enqueue_abort(Object key) {
        check_thread ();
	prepares.remove (key);
    }

    public void setEnqueuePredicate (EnqueuePredicateIF pred) {
	throw new NoSuchMethodError ("not supported by DustDevilSink.");
    }

    public EnqueuePredicateIF getEnqueuePredicate () {
	throw new NoSuchMethodError ("not supported by DustDevilSink.");
    }

    public int size () {
	throw new NoSuchMethodError ("not supported by DustDevilSink.");
    }

    public String toString () {
	return "(DustDevilSink hash_code=" + hash_code + ")";
    }

    public EventHandlerIF getHandler() {
        check_thread ();
	return handler;
    }

    protected void check_thread () {
        if (Thread.currentThread () != thread)
            throw new IllegalStateException ("may only be called from thread " 
                    + thread.getName ());
    }
}

