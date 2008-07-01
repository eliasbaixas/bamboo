/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */
package bamboo.sim;

import bamboo.lss.ASyncCore;
import java.nio.channels.SelectionKey;
import java.nio.channels.SelectableChannel;
import java.nio.channels.ClosedChannelException;

/**
 * Implements the bamboo.lss.ASyncCore.register_timer function by calling the
 * bamboo.sim.EventQueue.register_timer function, so that stages that use the
 * former can run under the simulator.
 *
 * @author Sean C. Rhea
 * @version $Id: SimulatedASyncCore.java,v 1.5 2005/07/01 00:07:56 srhea Exp $
 */
public class SimulatedASyncCore extends ASyncCore {

    protected EventQueue event_queue;

    public SimulatedASyncCore (EventQueue e) { event_queue = e; }

    public SelectionKey register_selectable(
        SelectableChannel channel, int interest_ops,
        SelectableCB cb, Object user_data) throws ClosedChannelException {
        throw new NoSuchMethodError("running under simulator");
    }

    public void unregister_selectable (SelectionKey skey) {
        throw new NoSuchMethodError("running under simulator");
    }

    public long nowMillis() {
        return (event_queue.now_us () + 999) / 1000; // round up
    }

    public long timerMillis() { return nowMillis(); }

    public Object registerTimer(long time_ms, final Runnable cb) {
        return event_queue.register_timer(event_queue.current_node_id(),
                time_ms * 1000, new EventQueue.Callback() {
                    public void call(Object notUsed) { cb.run(); }
                }, null);
    }

    public void cancelTimer (Object token) {
        event_queue.cancel_timer (token);
    }

    public void registerSelectable (
	    SelectableChannel channel, int interestOps, Runnable callback) 
            throws ClosedChannelException {
        throw new NoSuchMethodError("running under simulator");
    }

    public void unregisterSelectable (
	    SelectableChannel channel, int interestOps) 
            throws ClosedChannelException {
        throw new NoSuchMethodError("running under simulator");
    }

    public void unregisterSelectable (SelectableChannel channel) {
        throw new NoSuchMethodError("running under simulator");
    }

    public void asyncMain () {
        throw new NoSuchMethodError("running under simulator");
    }
}
