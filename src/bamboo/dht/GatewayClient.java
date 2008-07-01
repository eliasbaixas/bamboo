/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import ostore.util.NodeId;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SinkException;
import seda.sandStorm.api.SinkIF;
import bamboo.lss.ASyncCore;
import bamboo.util.XdrInputBufferDecodingStream;
import bamboo.util.XdrByteBufferEncodingStream;
import bamboo.lss.NioMultiplePacketInputBuffer;
import bamboo.sim.Simulator;
import org.acplt.oncrpc.XdrAble;
import org.acplt.oncrpc.XdrVoid;
import bamboo.util.XdrClone;
import bamboo.sim.EventQueue;
import bamboo.sim.Network;
import java.security.SecureRandom;
import static bamboo.util.Curry.*;

/**
 * An event-driven gateway client.
 *
 * @author Sean C. Rhea
 * @version $Id: GatewayClient.java,v 1.16 2005/07/01 00:11:35 srhea Exp $
 */
public class GatewayClient extends bamboo.util.StandardStage {

    /** Sets the client library field in the given request.
     *  
     **/
    private void set_client_library(Request req) {
        if (req.request instanceof bamboo_put_args) {
            bamboo_put_args put = (bamboo_put_args) req.request;
            put.client_library = 
                "bamboo.dht.GatewayClient $Revision: 1.16 $";
            assert req.vers == 2 : req.vers;
            assert req.proc == 2 : req.proc;
        } 
        else if (req.request instanceof bamboo_put_arguments) {
            bamboo_put_arguments put = 
                (bamboo_put_arguments) req.request;
            put.client_library = 
                "bamboo.dht.GatewayClient $Revision: 1.16 $";
            assert req.vers == 3 : req.vers;
            assert req.proc == 2 : req.proc;
        }
        else if (req.request instanceof bamboo_rm_arguments) {
            bamboo_rm_arguments rm = 
                (bamboo_rm_arguments) req.request;
            rm.client_library = 
                "bamboo.dht.GatewayClient $Revision: 1.16 $";
            assert req.vers == 3 : req.vers;
            assert req.proc == 4 : req.proc;
        }
        else if (req.request instanceof bamboo_get_args) {
            bamboo_get_args get = (bamboo_get_args) req.request;
            get.client_library = 
                "bamboo.dht.GatewayClient $Revision: 1.16 $";
            assert req.proc == 3 : req.proc;
        }
        else {
            assert false;
        }
    }

    public static Map<InetSocketAddress,GatewayClient> instances =
        new HashMap<InetSocketAddress,GatewayClient>();

    public static GatewayClient instance(InetSocketAddress addr) {
        return instances.get(addr);
    }

    protected void req(int vers, int proc, XdrAble req, Thunk1 resp) {
        handleEvent(new Request (vers, proc, req, resp));
    }

    public void putSecret(bamboo_put_arguments args, Thunk1<Integer> resp) {
        req(3, 2, args, resp);
    }

    public void getDetails(bamboo_get_args args, 
                           Thunk1<bamboo_get_result> resp) {
        req(3, 3, args, resp);
    }

    public void remove(bamboo_rm_arguments args, Thunk1<Integer> resp) {
        req(3, 4, args, resp);
    }

    public interface GetDoneCb {
        void get_done_cb (bamboo_get_res get_res, Object user_data);
    }

    public interface PutDoneCb {
        void put_done_cb (int put_res, Object user_data);
    }

    public interface RmDoneCb {
        void rm_done_cb (int rm_res, Object user_data);
    }

    public void get(bamboo_get_args get_args, final GetDoneCb cb, 
                    final Object user_data) {
        Thunk1 t = new Thunk1() { 
            public void run(Object resp) {
                cb.get_done_cb((bamboo_get_res) resp, user_data);
            }
        };
        req(2, 3, get_args, t);
    }

    public void get(bamboo_get_args get_args, 
                    final Thunk1<bamboo_get_res> thunk) {
        Thunk1 t = new Thunk1() { 
            public void run(Object resp) { thunk.run((bamboo_get_res) resp); }
        };
        req(2, 3, get_args, t);
    }

    public void put(bamboo_put_args put_args, final PutDoneCb cb, 
                    final Object user_data) {
        Thunk1 t = new Thunk1() { 
            public void run(Object resp) {
                cb.put_done_cb((Integer) resp, user_data);
            }
        };
        req(2, 2, put_args, t);
    }

    public void put(bamboo_put_args put_args, final Thunk1<Integer> thunk) {
        Thunk1 t = new Thunk1() { 
            public void run(Object resp) { thunk.run((Integer) resp); }
        };
        req(2, 2, put_args, t);
    }

    public void rm(bamboo_rm_args rm_args, RmDoneCb cb, Object user_data) {
        throw new NoSuchMethodError("no longer supported");
    }

    public void rm(bamboo_rm_args rm_args, final Thunk1<Integer> thunk) {
        throw new NoSuchMethodError("no longer supported");
    }

    protected static final int KEY_LEN = 20;

    protected SocketChannel channel;
    protected ByteBuffer read_buf = ByteBuffer.allocate (1500);
    protected ByteBuffer write_buf = ByteBuffer.allocate (16384);
    protected LinkedList<QueueElementIF> waiting = 
        new LinkedList<QueueElementIF> ();
    protected Map<Integer,Request> inflight = new HashMap<Integer,Request> ();
    protected int next_xact_id;
    protected NioMultiplePacketInputBuffer ib =
        new NioMultiplePacketInputBuffer ();
    protected int next_read_size = -1;
    protected InetSocketAddress gateway;
    protected SelectionKey skey;

    protected class MySelectableCb implements ASyncCore.SelectableCB {
        public void select_cb (SelectionKey skey, Object user_data) {
            logger.debug ("select_cb");
            if ((skey.readyOps () & skey.interestOps ()
                 & SelectionKey.OP_CONNECT) != 0) {
                logger.debug ("op_connect");
                try {
                    if (channel.finishConnect ()) {
                        if (waiting.isEmpty ())
                            skey.interestOps (SelectionKey.OP_READ);
                        else
                            skey.interestOps (SelectionKey.OP_READ
                                    | SelectionKey.OP_WRITE);
                    }
                }
                catch (Exception e) { 
                    try_next_gateway ();
                    return;
                }
                if (! waiting.isEmpty ())
                    skey.interestOps(skey.interestOps()|SelectionKey.OP_WRITE);
            }

            if ((skey.readyOps () & skey.interestOps ()
                 & SelectionKey.OP_WRITE) != 0) {
                logger.debug ("op_write");
                while (true) {
                    if (write_buf.position () < write_buf.limit ()) {
                        if (logger.isDebugEnabled ()) {
                            logger.debug ("sending:\n" +
                                    ostore.util.ByteUtils.print_bytes (
                                        write_buf.array (),
                                        write_buf.arrayOffset ()
                                        + write_buf.position (),
                                        write_buf.limit ()
                                        - write_buf.position ()));
                        }

                        try { channel.write (write_buf); }
                        catch (IOException e) { 
                            try_next_gateway ();
                            return;
                        }
                        if (write_buf.position () < write_buf.limit ())
                            break;

                        if (logger.isDebugEnabled ()) {
                            logger.debug (
                                    (write_buf.limit () -write_buf.position ())
                                    + " bytes remaining");
                        }
                    }
                    if (waiting.isEmpty ()) {
                        skey.interestOps (
                                skey.interestOps () & ~SelectionKey.OP_WRITE);
                        break;
                    }

                    write_buf.clear ();
                    write_buf.position (4); // reserve space for length

                    int xact_id = next_xact_id++;
                    if (logger.isDebugEnabled ())
                        logger.debug ("req xact_id=" 
                                + Integer.toHexString (xact_id));
                    write_buf.putInt (xact_id);
                    write_buf.putInt (0); // msg_type: CALL
                    write_buf.putInt (2); // rpcvers
                    write_buf.putInt (708655600); // prog

                    Request req = (Request) waiting.removeFirst ();
                    if (req.request instanceof XdrVoid) {
                        assert req.proc == 1;
                    }
                    else
                        set_client_library(req);

                    write_buf.putInt(req.vers);
                    write_buf.putInt(req.proc);

                    // the creditials and verifier
                    for (int i = 0; i < 2; ++i) {
                        write_buf.putInt (0); // flavor
                        write_buf.putInt (0); // length
                    }

                    XdrByteBufferEncodingStream es =
                        new XdrByteBufferEncodingStream (write_buf);
                    try {
                        if (req.request instanceof bamboo_put_args)
                            ((bamboo_put_args) req.request).xdrEncode (es);
                        else if (req.request instanceof bamboo_put_arguments)
                            ((bamboo_put_arguments) req.request).xdrEncode (es);
                        else if (req.request instanceof bamboo_get_args)
                            ((bamboo_get_args) req.request).xdrEncode (es);
                        else if (req.request instanceof bamboo_rm_arguments)
                            ((bamboo_rm_arguments) req.request).xdrEncode (es);
                        else
                            assert false;
                    }
                    catch (Exception e) { assert false : e; }

                    write_buf.flip ();
                    write_buf.putInt (0x80000000 | (write_buf.limit () - 4));
                    write_buf.position (0);

                    logger.debug ("size=" + write_buf.limit ());

                    inflight.put (new Integer (xact_id), req);
                }
            }

            if ((skey.readyOps () & skey.interestOps ()
                 & SelectionKey.OP_READ) != 0) {
                logger.debug ("op_read");
                while (true) {
                    int count = 0;
                    try { count = channel.read (read_buf); }
                    catch (IOException e) { 
                        try_next_gateway ();
                        return;
                    }
                    if (count < 0) {
                        try_next_gateway ();
                        return;
                    }
                    if (count > 0) {
                        read_buf.flip ();
                        if (logger.isDebugEnabled ()) {
                            logger.debug ("received:\n" +
                                    ostore.util.ByteUtils.print_bytes (
                                        read_buf.array (),
                                        read_buf.arrayOffset ()
                                        + read_buf.position (),
                                        read_buf.limit ()
                                        - read_buf.position ()));
                        }
                        ib.add_packet (read_buf);
                        read_buf = ByteBuffer.allocate (1500);
                    }

                    if (next_read_size == -1) {
                        if (ib.size () < 4)
                            break;
                        next_read_size = ib.nextInt () & 0x7fffffff;
                    }

                    if (ib.size () < next_read_size)
                        break;

                    if (logger.isDebugEnabled ())
                        logger.debug ("ib.size=" + ib.size ());

                    int xact_id = ib.nextInt ();
                    int rpc_reply = ib.nextInt ();
                    int msg_accepted = ib.nextInt ();
                    int auth_null = ib.nextInt ();
                    int auth_len = ib.nextInt ();
                    int success = ib.nextInt ();
                    assert success == 0;

                    if (logger.isDebugEnabled ())
                        logger.debug ("resp xact_id="
                                + Integer.toHexString (xact_id));

                    Request req = (Request)
                        inflight.remove (new Integer (xact_id));
                    assert req != null;

                    Object resp = null;
                    if ((req.request instanceof bamboo_put_args)
                         || (req.request instanceof bamboo_put_arguments)
                         || (req.request instanceof bamboo_rm_arguments)) {
                        resp = new Integer(ib.nextInt());
                    }
                    else { 
                        assert req.request instanceof bamboo_get_args;
                        XdrInputBufferDecodingStream ds =
                            new XdrInputBufferDecodingStream (ib, ib.size ());
                        try {
                            ds.beginDecoding ();
                            if (req.vers == 2) 
                                resp = new bamboo_get_res (ds);
                            else 
                                resp = new bamboo_get_result (ds);
                        }
                        catch (Exception e) { BUG (e); }
                    }

                    if (req.completion_queue == null)
                        ((Thunk1) req.user_data).run(resp);
                    else 
                        application_enqueue (req.completion_queue,
                                new Response (resp, req.user_data));

                    next_read_size = -1;
                }
            }
        }
    }

    public void put_done (int xact_id, int result) {
        Request req = (Request) inflight.remove(new Integer(xact_id));
        Integer resp = new Integer(result);
        application_enqueue(req.completion_queue,
                            new Response(resp, req.user_data));
    }

    public void rm_done(int xact_id, int result) {
        Request req = (Request) inflight.remove(new Integer(xact_id));
        Integer resp = new Integer(result);
        application_enqueue(req.completion_queue,
                            new Response(resp, req.user_data));
    }

    public void get_done (int xact_id, bamboo_get_res res) {
        Request req = (Request) inflight.remove(new Integer(xact_id));
        application_enqueue(req.completion_queue,
                            new Response(res, req.user_data));
    }

    protected void application_enqueue (SinkIF sink, QueueElementIF item) {
        if (logger.isDebugEnabled ())
            logger.debug ("enqueuing " + item + " to " + sink);
        try { sink.enqueue (item); }
        catch (SinkException e) { BUG (e); }
    }

    public GatewayClient () {
        write_buf.limit (0);
    }

    protected Gateway gway_inst;
    protected Simulator simulator;
    protected NodeId gnid;
    protected static ByteBuffer bbuf = ByteBuffer.allocate (16384);
    protected LinkedList<NodeId> gateways = new LinkedList<NodeId> ();
    protected NodeId last_gw;

    public void init (ConfigDataIF config) throws Exception {
        super.init(config);
        instances.put(my_node_id, this);

        int cnt = config_get_int (config, "gateway_count");
        if (cnt == -1) {
            gateways.addLast (new NodeId (
                        config_get_string (config, "gateway")));
        }
        else { 
            for (int i = 0; i < cnt; ++i) {
                gateways.addLast (new NodeId (
                            config_get_string (config, "gateway_" + i)));
            }
        }
        last_gw = gateways.getLast ();

        if (sim_running) {
            gnid = gateways.getFirst ();
            logger.info ("gid=" + gnid);
            gway_inst = (Gateway) Gateway.instances.get (gnid);
            if (gway_inst == null) {
                logger.fatal("can't find gateway");
                System.exit(1);
            }
            simulator = Simulator.instance ();
        }
        else {
            try_next_gateway ();
        }
        SecureRandom rand = SecureRandom.getInstance("SHA1PRNG");
        next_xact_id = rand.nextInt ();
    }

    protected void try_next_gateway () {
        // We want to immediately try and connect to the first gateway.  If
        // that fails, we try each of the rest in order.  Once we've made it
        // through the list, though, we pause for five seconds before starting
        // over.  This change is so that we don't go nuts when our local
        // network is down.
        if (skey != null) {
            acore.unregister_selectable (skey);
            skey = null; channel = null; gateway = null;
        }
        boolean sleep_first = (gnid != null) && gnid.equals (last_gw);
        if (sleep_first) {
            logger.info ("Waiting five seconds before connecting");
            acore.register_timer (5000, try_next_gateway_cb);
        }
        else 
            try_next_gateway_cb.run();
    }

    protected Runnable try_next_gateway_cb = new Runnable () {
        public void run() {
        gnid = (NodeId) gateways.removeFirst ();
        gateways.addLast (gnid);
        logger.info ("Trying to connect to gateway " + gnid);
        try {
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            gateway = new InetSocketAddress(gnid.address(), gnid.port());
            channel.connect(gateway); 
            assert acore != null;
            skey = acore.register_selectable(channel, SelectionKey.OP_CONNECT,
                    new MySelectableCb(), null);
        }
        catch (IOException e) { BUG (e); } 
        for (Integer xact_id : inflight.keySet ()) 
            waiting.addLast (inflight.get (xact_id));
        inflight = new HashMap<Integer,Request> ();
    }
    };
    
    public static class Request implements QueueElementIF {
        public int vers, proc;
        public XdrAble request;
        public SinkIF completion_queue;
        public Object user_data;
        public Request (XdrAble r, SinkIF c, Object u) {
            request = r; completion_queue = c; user_data = u; vers = 2; 
            if (request instanceof XdrVoid)
                proc = 1;
            else if (request instanceof bamboo_put_args)
                proc = 2;
            else if (request instanceof bamboo_get_args)
                proc = 3;
            else
                assert false : request.getClass().getName();
        }
        public Request(int v, int p, XdrAble r, Thunk1 u) {
            vers = v; proc = p; request = r; user_data = u;
        }
    }

    public static class Response implements QueueElementIF {
        public Object response;
        public Object user_data;
        public Response (Object r, Object u) { response = r; user_data = u; }
    }

    public void handleEvent (QueueElementIF item) {
	if (logger.isDebugEnabled ()) logger.debug ("got " + item);
        if (item instanceof Request) {
            if (sim_running) {
                final int xact_id = next_xact_id++;
                Request req = (Request) item;
                inflight.put (new Integer (xact_id), req);
                set_client_library (req);
                final XdrAble clone = XdrClone.xdr_clone (req.request, bbuf);
                int size = bbuf.position ();
                long lat =
                    Network.msg_latency_us (my_node_id, gnid, size) / 1000;
                final GatewayClient self = this;
                EventQueue.Callback cb = new EventQueue.Callback () {
                    int id = xact_id;
                    XdrAble arg = clone;
                    public void call (Object not_used) {
                        if (arg instanceof bamboo_put_args) {
                            gway_inst.do_put (id, (bamboo_put_args) arg,
                                              my_node_id, self);
                        }
                        else {
                            assert arg instanceof bamboo_get_args;
                            gway_inst.do_get (id, (bamboo_get_args) arg,
                                              my_node_id, self);
                        }
                    }

                };
                // TODO: what if the Gateway dies before it receives this?
                simulator.event_queue.
                    register_timer(gway_inst.my_node_id (), lat, cb, clone);
            }
            else {
                waiting.addLast(item);
                if ((skey != null) && skey.isValid ())
                    skey.interestOps(skey.interestOps()|SelectionKey.OP_WRITE);
            }
        }
        else {
            Response resp = (Response) item;
            if (resp.user_data instanceof Thunk1) {
                Thunk1 cb = (Thunk1) resp.user_data;
                cb.run((XdrAble) resp.response);
            }
            else {
                Object [] ud = (Object []) resp.user_data;
                Object user_data = ud [1];
                if (ud [0] instanceof GetDoneCb) {
                    GetDoneCb cb = (GetDoneCb) ud [0];
                    cb.get_done_cb ((bamboo_get_res) resp.response, user_data);
                }
                else if (ud [0] instanceof PutDoneCb) {
                    PutDoneCb cb = (PutDoneCb) ud [0];
                    cb.put_done_cb (((Integer)resp.response).intValue(), 
                                     user_data);
                }
                else {
                    assert false : ud [0].getClass ().getName ();
                }
            }
        }
    }
}

