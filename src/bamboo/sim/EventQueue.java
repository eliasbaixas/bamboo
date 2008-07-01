/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.sim;
import bamboo.lss.PriorityQueue;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.log4j.Logger;
import ostore.util.NodeId;

/**
 * A really simple, and hopefully really fast, simulator core.  The user
 * can add nodes with a specified lifetime, register and cancel timer
 * events, and nothing more.  Timers that would go off after a node has
 * been removed are ignored at registration time.  Also, a cleanup function
 * may be provided when adding a node; it will be called before the node is
 * removed.  Time is in microseconds.
 *
 * @author Sean C. Rhea
 * @version $Id: EventQueue.java,v 1.11 2005/08/30 01:03:16 srhea Exp $
 */
public class EventQueue {

    protected static Logger logger = Logger.getLogger (EventQueue.class);

    protected Map<NodeId,NodeInfo> nodes_by_node_id = 
        new HashMap<NodeId,NodeInfo>();
    protected Set<TimerInfo> cancelled_timers = new HashSet<TimerInfo>();
    protected PriorityQueue event_queue = new PriorityQueue (1000);
    protected long now_us;
    protected NodeId current_node_id;

    public static interface Callback { void call (Object user_data); }

    public void add_node (NodeId node_id, long life_time_us,
	    Callback cleanup_cb, Object user_data) {
	if (nodes_by_node_id.containsKey (node_id)) {
	    throw new IllegalStateException (
		    "already have a node with id " + node_id);
	}
	else {
	    long failure_time_us = now_us + life_time_us;
	    NodeInfo ninfo = new NodeInfo (
		    node_id, failure_time_us, cleanup_cb, user_data);
	    nodes_by_node_id.put (node_id, ninfo);
	    TimerInfo tinfo = new TimerInfo (null, failure_cb, node_id);
	    event_queue.add (tinfo, failure_time_us);
	}
    }

    public Object register_timer (
	    long time_us, Callback cb, Object user_data) {
	long call_time_us = now_us + time_us;
	TimerInfo result = new TimerInfo (null, cb, user_data);
	event_queue.add (result, call_time_us);
	return result;
    }

    public Object register_timer (
	    NodeId node_id, long time_us, Callback cb, Object user_data) {
        assert time_us >= 0 : "time_us=" + time_us;
        
	long call_time_us = now_us + time_us;
	NodeInfo ninfo = nodes_by_node_id.get(node_id);
        assert ninfo != null : node_id;

        if (logger.isDebugEnabled ())
            logger.debug ("now_us=" + now_us + " call_time_us=" +
                    call_time_us + " fail_time=" + ninfo.failure_time_us);
        assert call_time_us >= now_us :
            node_id + " registered an event in the past! time_us=" +
            time_us;
	if (call_time_us < ninfo.failure_time_us) {
	    TimerInfo result = new TimerInfo (node_id, cb, user_data);
	    event_queue.add (result, call_time_us);
	    return result;
	}
	else
	    return null;
    }

    public void cancel_timer (Object token) {
	if (token != null) {
	    if (! (token instanceof TimerInfo))
		throw new IllegalArgumentException ();
	    cancelled_timers.add((TimerInfo) token);
	}
    }

    public void run () {
	while (! event_queue.isEmpty ()) {
	    now_us = event_queue.getFirstPriority ();
	    TimerInfo tinfo = null;
            while (true) {
                tinfo = (TimerInfo) event_queue.removeFirst ();
                if (!cancelled_timers.remove(tinfo))
                    break;
            }
            current_node_id = tinfo.node_id;
	    tinfo.cb.call (tinfo.user_data);
	}
    }

    public long failure_time_us (NodeId node_id) {
	NodeInfo ninfo = nodes_by_node_id.get(node_id);
	if (ninfo == null)
	    return -1;
        return ninfo.failure_time_us;
    }

    public long now_us () {
        return now_us;
    }

    public NodeId current_node_id () {
        return current_node_id;
    }

    public void set_current_node_id (NodeId n) {
        current_node_id = n;
    }

    protected static class NodeInfo {
	public NodeId node_id;
	public long failure_time_us;
	public Callback cleanup_cb;
	public Object cleanup_user_data;
	public NodeInfo (NodeId n, long f, Callback cb, Object ud) {
	    node_id = n; failure_time_us = f; cleanup_cb = cb;
	    cleanup_user_data = ud;
	}
    }

    protected static class TimerInfo {
        public NodeId node_id;
	public Callback cb;
	public Object user_data;
	public TimerInfo (NodeId n, Callback c, Object u) {
	    node_id = n; cb = c; user_data = u;
	}
    }

    protected Callback failure_cb = new Callback () {
	public void call (Object user_data) {
	    NodeId node_id = (NodeId) user_data;
	    NodeInfo ninfo = nodes_by_node_id.get(node_id);
	    ninfo.cleanup_cb.call (ninfo.cleanup_user_data);
            if (logger.isDebugEnabled ())
                logger.debug ("removing " + node_id + " from nodes");
            // People may send messages to nodes that have died.
            nodes_by_node_id.remove (node_id);
	}
    };
}

