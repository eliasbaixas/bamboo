/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.lss;
import java.net.InetSocketAddress;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Random;
import ostore.network.NetworkMessage;
import ostore.network.NetworkMessageResult;
import ostore.network.NetworkLatencyReq;
import ostore.network.NetworkLatencyResp;
import ostore.util.CountBuffer;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QuickSerializable;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SinkException;
import seda.sandStorm.api.SinkIF;
import static bamboo.util.Curry.*;

/**
 * Wraps bamboo.lss.UdpCC in the ostore.network.Network interface.
 *
 * @author Sean C. Rhea
 * @version $Id: Network.java,v 1.35 2005/06/30 22:30:55 srhea Exp $
 */
public class Network extends bamboo.util.StandardStage
implements UdpCC.Serializer, UdpCC.Sink {

    protected static final int DEFAULT_TIMEOUT = 60;

    /////////////////// Function-Callback Interface //////////////////

    protected static LinkedHashMap<InetSocketAddress,Network> instances = 
        new LinkedHashMap<InetSocketAddress,Network>();

    public static Network instance(InetSocketAddress addr) {
        return (Network) instances.get (addr);
    }

    // This is some hairy code, mainly because of all of the type parameter
    // stuff.  Try to be calm while reading it.  Our goal is that a client of
    // this class not be able to call registerReceiver with a class that is
    // not the same as the type of the first argument of the given thunk.  For
    // instance, one shouldn't be able to say:
    //
    //     registerReceiver(Integer.class, new Thunk6<Long,...>() { ... });
    //
    // but only things like this:
    //
    //     registerReceiver(Integer.class, new Thunk6<Integer,...>() { ... });
    //
    // That way we know that the cast in the first statement of recv() will
    // work at runtime.
    //
    // Ideally, I'd like to find a way to remove the cast in recv()
    // altogether (so that this class won't generate any unchecked cast
    // warnings at runtime, but I haven't found a way to do it in Java's type
    // system.

    protected LinkedHashMap<Class<? extends QuickSerializable>, Thunk2<? extends QuickSerializable, InetSocketAddress>> receivers = new LinkedHashMap<Class<? extends QuickSerializable>, Thunk2<? extends QuickSerializable, InetSocketAddress>>();

    /**
     * Register to receive network messages of a particular type.  There can
     * be only one receiver per type, and all types must be subtypes of
     * <code>ostore.util.QuickSerializable</code>.
     *
     * @param type the class of the messages to be received
     *
     * @param callback the function to call when a message of class
     * <code>type</code> is received.  The arguments are the message itself 
     * adn the remote node's IP and port.
     */
    public <T extends QuickSerializable> void registerReceiver(
            Class<T> type, Thunk2<T, InetSocketAddress> callback)
    throws DuplicateTypeException {
        if (receivers.containsKey(type)) 
            throw new DuplicateTypeException (type);
        try { ostore.util.TypeTable.register_type(type); }
        catch (Exception e) { BUG(e); }
        receivers.put(type, callback);
    }

    /**
     * Returns the estimated round-trip latency to the given peer, or -1 if
     * the no estimate is available.  (If you send a message to the peer and
     * it is successful, there will be an estimate available if this function
     * is called from the result callback.)
     */
    public long estimatedRTTMillis(InetSocketAddress peer) {
        assert udpcc_thread == null; // we never run this way anymore...
        return udpcc.latency_mean (peer);
    }

    protected Thunk1<Boolean> nullSendCallback = new Thunk1<Boolean>() {
        public void run(Boolean notUsed) {}
    };

    protected Object bogusToken = new Object();

    /**
     * Send <code>msg</code> to <code>dst</code> with the default timeout and
     * no notification of success or failure.
     */
    public Object send(QuickSerializable msg, InetSocketAddress dst) {
        return send(msg, dst, DEFAULT_TIMEOUT);
    }

    /**
     * Send <code>msg</code> to <code>dst</code> with the default timeout and
     * call <code>callback</code> with the result, where <code>true</code>
     * indicates success and <code>false</code> indicates failure.
     */
    public Object send(QuickSerializable msg, InetSocketAddress dst,
                     Thunk1<Boolean> callback) {
        return send(msg, dst, DEFAULT_TIMEOUT, callback);
    }

    /**
     * Send <code>msg</code> to <code>dst</code> if we can do so within
     * <code>timeoutSeconds</code> seconds.
     */
    public Object send(QuickSerializable msg, InetSocketAddress dst,
                     long timeoutSeconds) {
        return send(msg, dst, timeoutSeconds, nullSendCallback);
    }

    /**
     * Send <code>msg</code> to <code>dst</code> if we can do so within
     * <code>timeoutSeconds</code> seconds and call <code>callback</code> with
     * the result, where <code>true</code> indicates success and
     * <code>false</code> indicates failure.
     */
    public Object send(final QuickSerializable msg, final InetSocketAddress dst,
                       final long timeoutSeconds, 
                       final Thunk1<Boolean> callback) {

        if (drop_prob > 0.0) {
            double dp = 1.0;
            long ts = timeoutSeconds;
            do {
                dp *= drop_prob;
                ts -= 5;
            }
            while (ts > 0);

            if (rand.nextDouble() < dp) {
                if (callback != null) {
                    acore.registerTimer(timeoutSeconds * 1000, 
                                        curry(callback, new Boolean(false)));
                }
                return bogusToken;
            }
        }

        if (udpcc_thread == null) {
            return udpcc.send(msg, dst, timeoutSeconds, callback);
        }

        // Go into the UdpCC thread...
        udpcc_thread.registerTimer(0, new Runnable() {
            public void run() {
                // ...and call send...
                udpcc.send(msg, dst, timeoutSeconds, new Thunk1<Boolean>() {
                    public void run(final Boolean success) {
                        // ...and when it's done, come back into the main
                        // thread...
                        acore.registerTimer(0, new Runnable() {
                            public void run() {
                                // ...and then call the supplied callback.
                                callback.run(success);
                            }
                        });
                    }
                });
            }
        });
        return null; // cancelSend not supported if UdpCC in its own thread
    }

    /**
     * Cancel a send in progress.  When send is called, it returns a token; if
     * this function is called with that token, then the corresponding message
     * will not be sent if it hasn't already been sent, and the callback
     * passed to send will not be called if it hasn't already been called.
     */
    public void cancelSend(Object token) {
        assert udpcc_thread == null : 
            "cancelSend not supported if UdpCC in its own thread";
        if (token != bogusToken)
            udpcc.cancelSend(token);
    }

    //////////////// End of Function-Callback Interface //////////////

    protected void enqueue_to_main_thread (
            final SinkIF sink, final QueueElementIF item) {

        if (udpcc_thread == null) {
            if (logger.isDebugEnabled ()) 
                logger.debug ("enqueuing " + item);
            try {
                sink.enqueue (item);
            }
            catch (SinkException e) {
                BUG ("could not enqueue " + item, e);
            }
        }
        else {
            ASyncCore.TimerCB cb = new ASyncCore.TimerCB () {
                public void timer_cb (Object user_data) {
                    if (logger.isDebugEnabled ()) 
                        logger.debug ("enqueuing " + item);
                    try {
                        sink.enqueue (item);
                    }
                    catch (SinkException e) {
                        BUG ("could not enqueue " + item, e);
                    }
                }
            };
            main_acore.register_timer (0, cb, null);
        }
    }

    protected InetSocketAddress addr;
    protected ASyncCore main_acore;
    protected double drop_prob;
    protected Random rand;

    public Network (InetSocketAddress addr, ASyncCore ac) throws IOException {
        this.addr = addr;
        main_acore = ac;

        instances.put (addr, this);

        event_types    = new Class [] { NetworkLatencyReq.class };
	outb_msg_types = new Class [] { NetworkMessage.class };

	// This output line makes OceanStore scripts think we're ready to
	// receive messages, which allows run-experiment (in particular) to
	// work correctly when a DustDevil node is specified as being of
	// type fed (as opposed to dynamic or static).

	logger.info ("Network " + addr.getAddress ().getHostName () +
		" now listening on port " + addr.getPort ());
    }

    /////////////////// Implementation of Serializer ////////////////

    public int serialize_size (Object msg) {
	CountBuffer cb = new CountBuffer ();
	cb.add ((QuickSerializable) msg);
	return cb.size ();
    }

    public void serialize (Object msg, ByteBuffer buf) {
	OutputBuffer ob = new NioOutputBuffer (buf);
	ob.add ((QuickSerializable) msg);
    }

    public Object deserialize (ByteBuffer buf) throws Exception {
	InputBuffer ib = new NioInputBuffer (buf);
	return ib.nextObject ();
    }

    ///////////////// End Implementation of Serializer //////////////

    ////////////////////// Implementation of Sink ///////////////////

    public void recv (final Object o, final InetSocketAddress src, 
                      final InetSocketAddress l, final int tries, 
                      final long wait_ms, final long est_rtt_ms) {

        final Thunk2<QuickSerializable,InetSocketAddress> callback = 
            (Thunk2<QuickSerializable,InetSocketAddress>)
            receivers.get(o.getClass());

        if (callback != null) {
            final QuickSerializable qs = (QuickSerializable) o;
            if (udpcc_thread == null) {
                callback.run(qs, src);
            }
            else {
                // Get back into the main thread.
                Runnable r =  new Runnable() { 
                    public void run() { callback.run(qs, src); } 
                };
                acore.registerTimer(0, r);
            }
        }
        else {
            final NetworkMessage msg = (NetworkMessage) o;
            msg.inbound = true;
            msg.peer = NodeId.create(src);
            msg.wait_ms = wait_ms;
            msg.est_rtt_ms = est_rtt_ms;

            // Use dispatch later to get back into main thread.
            if (udpcc_thread == null) {
                try { classifier.dispatch(msg); }
                catch (SinkException e) { BUG(e); }
            }
            else{
                classifier.dispatch_later (msg, 0);
            }
        }
    }

    //////////////////// End Implementation of Sink /////////////////

    ///////////////// Implementation of EventHandlerIF //////////////

    public void handleEvent (QueueElementIF item) {
        if (item instanceof NetworkMessage) {
            handle_outbound_msg ((NetworkMessage) item);
        }
        else {
            handle_net_lat_req ((NetworkLatencyReq) item);
        }
    }

    public void init (ConfigDataIF config) throws Exception {
        super.init (config);

        // Under the simulator, everything is handled by bamboo.sim.Network,
        // which must inherit from us to get our interface (and the stuff
        // from bamboo.util.StandardStage), but otherwise doesn't use this
        // class at all.
        if (sim_running)
            return;

        drop_prob = configGetDouble(config, "drop_prob", 0.0);
        if (drop_prob > 0.0)
            rand = new Random(my_node_id.hashCode() ^ now_ms());

        boolean separate_thread = config_get_boolean(config, "separate_thread");
        if (separate_thread) {
            udpcc_thread = new ASyncCoreImpl ();
	    udpcc = new UdpCC (udpcc_thread, addr, this, this);
        }
        else {
	    udpcc = new UdpCC (main_acore, addr, this, this);
        }

	int i = config_get_int (config, "udpcc_debug_level");
	if (i >= 0)
	    udpcc.set_debug_level (i);

        i = config_get_int (config, "udpcc_rx_sockbuf_size");
        if (i > 0)
            udpcc.set_sockbuf_size (i);

        double timeout_factor = config_get_double (config, "timeout_factor");
        if (timeout_factor != -1.0)
            udpcc.set_timeout_factor (timeout_factor);

        double timeout_diff = config_get_double (config, "timeout_diff");
        if (timeout_diff != -1.0)
            udpcc.set_timeout_diff (timeout_diff);

        String mackeyfile = config_get_string (config, "mac_key_file");
        if (mackeyfile != null)
            udpcc.set_mac_key (mackeyfile);

        // Don't start Network thread until init () has finished.

        if (separate_thread) {
            acore.register_timer (0, new ASyncCore.TimerCB () {
                public void timer_cb (Object user_data) {
                    Thread t = new Thread () {
                        public void run () {
                            try {
                                udpcc_thread.async_main ();
                            }
                            catch (Throwable e) {
                                logger.fatal ("uncaught exception", e);
                                System.exit (1);
                            }
                        }
                    };
                    t.start ();
                }
            }, null);
        }
    }

    ////////////// End of Implementation of EventHandlerIF ///////////

    protected class MySendCB implements UdpCC.SendCB {

        protected SinkIF comp_q;
        protected Object user_data;

        public MySendCB (SinkIF c, Object u) {
            comp_q = c;  user_data = u;
        }

        public void cb (Object not_used, boolean success) {
            NetworkMessageResult outb =
                new NetworkMessageResult (user_data, success);
            enqueue_to_main_thread (comp_q, outb);
        }
    }

    protected ASyncCore udpcc_thread;
    protected UdpCC udpcc;

    protected final void handle_outbound_msg (final NetworkMessage msg) {
        final InetSocketAddress dst = new InetSocketAddress (
                msg.peer.address (), msg.peer.port ());

        if (msg.timeout_sec == -1) {
            if (udpcc_thread == null) {
                udpcc.send_nocc (msg, dst);
            }
            else {
                udpcc_thread.registerTimer(0, new Runnable() {
                    public void run() { udpcc.send_nocc(msg, dst); }
                });
            }
        }
        else {
            final SinkIF comp_q = msg.comp_q;
            final Object user_data = msg.user_data;
            final long timeout_sec = 
                (msg.timeout_sec == 0) ? DEFAULT_TIMEOUT : msg.timeout_sec;
            final Thunk1<Boolean> callback = (msg.comp_q == null) ? null :
                new Thunk1<Boolean>() {
                    public void run(Boolean success) {
                        NetworkMessageResult outb =
                            new NetworkMessageResult (user_data, success);
                        enqueue_to_main_thread (comp_q, outb);
                    }
                };
            send(msg, dst, timeout_sec, callback);
        }
    }

    protected void handle_net_lat_req (final NetworkLatencyReq req) {
        final InetSocketAddress addr = new InetSocketAddress (
                req.node_id.address (), req.node_id.port ());
        ASyncCore.TimerCB cb = new ASyncCore.TimerCB () {
            public void timer_cb (Object not_used) {
                long rtt_ms = udpcc.latency_mean (addr);
                boolean success = (rtt_ms != -1L);
                NetworkLatencyResp resp =
                    new NetworkLatencyResp (success, rtt_ms, req.user_data);
                enqueue_to_main_thread (req.comp_q, resp);
            }
        };
        if (udpcc_thread == null) {
            cb.timer_cb (null);
        }
        else {
            udpcc_thread.register_timer (0, cb, null);
        }
    }
}


