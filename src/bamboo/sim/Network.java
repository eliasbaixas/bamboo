/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.sim;
import bamboo.lss.ASyncCore;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import ostore.network.NetworkLatencyReq;
import ostore.network.NetworkLatencyResp;
import ostore.network.NetworkMessage;
import ostore.network.NetworkMessageResult;
import ostore.util.QuickSerializable;
import ostore.util.QSClone;
import ostore.util.CountBuffer;
import ostore.util.NodeId;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SinkException;
import seda.sandStorm.api.SinkIF;
import static bamboo.util.Curry.*;

/**
 * A network stage for simulated machines.
 *
 * @author Sean C. Rhea
 * @version $Id: Network.java,v 1.7 2005/07/01 00:08:55 srhea Exp $
 */
public class Network extends bamboo.lss.Network {

    protected static Map<NodeId,SinkIF> sinks = new HashMap <NodeId,SinkIF>();
    protected static Logger logger = Logger.getLogger (Network.class);
    static {
         //logger.setLevel (Level.DEBUG);
    }

    protected Simulator simulator;
    protected int header_size = 40; // TODO
    protected int ack_size = header_size + 8;

    public Network(InetSocketAddress addr, ASyncCore ac) throws IOException {
        super(addr, ac);
        simulator = Simulator.instance ();
        if (simulator == null) BUG ("no simulator");
    }

    public void init (ConfigDataIF config) throws Exception {
	super.init (config);
	sinks.put (my_node_id, my_sink);
    }

    public void destroy() {
        super.destroy();
        sinks.remove(my_node_id);
    }

    public void handleEvent(QueueElementIF item) {
        if (item instanceof NetworkMessage) {
            NetworkMessage msg = (NetworkMessage) item;
            if (msg.inbound) {
                try { classifier.dispatch (msg); }
                catch (Exception e) { assert false : e; }
            }
            else {
                handle_network_message (msg);
            }
        }
        else if (item instanceof NetworkLatencyReq) {
            handle_network_latency_req ((NetworkLatencyReq) item);
        }
        else {
            throw new IllegalArgumentException (item.getClass ().getName ());
        }
    }

    protected static class EnqueueCb implements EventQueue.Callback {
        public QueueElementIF item;
        public SinkIF sink;
        public EnqueueCb (QueueElementIF i, SinkIF s) { item = i; sink = s; }
        public void call (Object user_data) {
            try { sink.enqueue (item); }
            catch (SinkException e) {
                System.err.println ("SinkException " + e);
                e.printStackTrace ();
                System.exit (1);
            }
        }
    }
    
    protected static final long msg_latency_us (
            NetworkModel.RouteInfo ri, int sz) {
        return Math.round (Math.ceil (ri.inv_bw_us_per_byte * sz))
            + ri.latency_us;
    }

    public static long msg_latency_us (NodeId a, NodeId b, int sz) {
        NetworkModel.RouteInfo ri = route_info (a, b);
        return msg_latency_us (ri, sz);
    }

    public static long network_latency_us (NodeId a, NodeId b) {
        NetworkModel.RouteInfo ri = route_info (a, b);
        return (long) (ri.latency_us);
    }

    protected static NetworkModel.RouteInfo route_info (NodeId a, NodeId b) {
        Simulator simulator = Simulator.instance ();
        int src = simulator.node_id_to_graph_index (a);
        int dst = simulator.node_id_to_graph_index (b);
        assert src != -1;
        assert dst != -1;
        return simulator.network_model.compute_route_info (src, dst);
    }

    protected void handle_network_latency_req (NetworkLatencyReq req) {
        long rtt_ms = (2*network_latency_us (my_node_id, req.node_id)/1000);
        NetworkLatencyResp resp =
            new NetworkLatencyResp (true, rtt_ms, req.user_data);
        try {
            req.comp_q.enqueue (resp);
        }
        catch (SinkException e) {
            BUG ("SinkException " + e);
        }
    }

    public Object send(final QuickSerializable msg, final InetSocketAddress d,
                       final long timeoutSeconds, 
                       final Thunk1<Boolean> callback) {

        NodeId peer = (NodeId) d;

        long dst_failure_time_us = simulator.event_queue.failure_time_us(peer);
        int src = simulator.node_id_to_graph_index (my_node_id);
        int dst = simulator.node_id_to_graph_index (peer);

        NetworkModel.RouteInfo ri =
            simulator.network_model.compute_route_info (src, dst);

        CountBuffer cb = new CountBuffer ();
        cb.add (msg);
        int msg_size = cb.size () + header_size;

        long route_lat_us = msg_latency_us (ri, msg_size);
        assert route_lat_us >= 0 : 
            "Bad route info from " + my_node_id + " to " + peer;
        long recv_time_us = simulator.event_queue.now_us () + route_lat_us;

        if (recv_time_us < dst_failure_time_us) {
            final QuickSerializable qs = QSClone.qs_clone(msg);
            if (qs instanceof NetworkMessage) {
                NetworkMessage nm = (NetworkMessage) qs;
                nm.peer = my_node_id;
                nm.inbound = true;
                nm.est_rtt_ms = route_lat_us * 1000;
            }

            final Thunk2<QuickSerializable,InetSocketAddress> recv_cb = 
                (Thunk2<QuickSerializable,InetSocketAddress>)
                ((Network)instance(peer)).receivers.get(msg.getClass());

            if (recv_cb != null) {
                simulator.event_queue.register_timer (peer, route_lat_us,
                        new EventQueue.Callback() { 
                            public void call(Object notUsed) {
                                recv_cb.run(qs, my_node_id);
                            }
                        }, null);
            } 
            else {
                assert msg instanceof NetworkMessage : 
                    "non-NetworkMessage type " + msg.getClass().getName() 
                    + " sent via send() without registered receiver";
                SinkIF dst_sink = (SinkIF) sinks.get (peer);
                simulator.event_queue.register_timer (peer, route_lat_us,
                    new EnqueueCb((NetworkMessage) qs, dst_sink), null);
            }

            long ack_time_us = route_lat_us + msg_latency_us (ri, ack_size);
            simulator.event_queue.register_timer (my_node_id, ack_time_us,
                        new EventQueue.Callback() { 
                            public void call(Object notUsed) {
                                callback.run(new Boolean(true));
                            }
                        }, null);
        }
        else {
            long total_timeout = timeoutSeconds * 1000 * 1000;
            simulator.event_queue.register_timer (my_node_id, total_timeout,
                        new EventQueue.Callback() { 
                            public void call(Object notUsed) {
                                callback.run(new Boolean(false));
                            }
                        }, null);
        }

        return bogusToken;  // TODO: allow cancelling in the simulator?
    }

    protected void handle_network_message (final NetworkMessage msg) {
        send(msg, msg.peer, msg.timeout_sec, new Thunk1<Boolean>() {
                public void run(Boolean success) {
                    if (msg.comp_q != null) {
                        try {
                            msg.comp_q.enqueue(new NetworkMessageResult(
                                    msg.user_data, success.booleanValue()));
                        }
                        catch (SinkException e) { assert false; }
                    }
                }
            });
    }
}

