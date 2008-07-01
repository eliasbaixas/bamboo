/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht;
import bamboo.db.StorageManager;
import bamboo.lss.ASyncCore;
import bamboo.lss.NioMultiplePacketInputBuffer;
import bamboo.sim.EventQueue;
import bamboo.sim.Network;
import bamboo.sim.Simulator;
import bamboo.util.GuidTools;
import bamboo.util.XdrByteBufferEncodingStream;
import bamboo.util.XdrClone;
import bamboo.util.XdrInputBufferDecodingStream;
import bamboo.www.WebInterface;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Vector;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrAble;
import org.acplt.oncrpc.XdrDecodingStream;
import org.acplt.oncrpc.XdrEncodingStream ;
import org.acplt.oncrpc.XdrInt;
import org.acplt.oncrpc.XdrVoid;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.Pair;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import static bamboo.util.Curry.*;
import static bamboo.util.StringUtil.bytes_to_sbuf;
import static java.nio.channels.SelectionKey.*;
import static bamboo.db.StorageManager.ZERO_KEY;
import static bamboo.dht.Dht.GetValue;
import static java.lang.Math.min;

/**
 * A gateway to access the DHT using Sun RPC over TCP.
 *
 * @author Sean C. Rhea
 * @version $Id: Gateway.java,v 1.54 2006/01/23 23:58:31 srhea Exp $
 */
public class Gateway extends bamboo.util.StandardStage
implements SingleThreadedEventHandlerIF {

    /////////////////////////////////////////////////////////////////
    //                                                             //
    //                         Accept Loop                         //
    //                                                             //
    /////////////////////////////////////////////////////////////////

    protected ServerSocket ssock;
    protected ServerSocketChannel ssock_channel;
    protected int server_port;
    protected boolean allow_unauth_rm;

    protected Runnable accept_cb = new Runnable () {
        public void run() {
            SocketChannel sc = null;
            try { sc = ssock_channel.accept (); }
            catch (IOException e) { BUG (e); }
            // Sometimes, even though isAcceptable is true, there still
            // isn't a connection to accept.
            if (sc == null) return;
            if (logger.isInfoEnabled ()) {
                Socket s = sc.socket ();
                StringBuffer buf = new StringBuffer (45);
                buf.append ("got connection from ");
                buf.append (s.getInetAddress ().getHostAddress ());
                buf.append (":");
                buf.append (s.getPort ());
                logger.info (buf);
            }
            try { sc.configureBlocking (false); }
            catch (IOException e) { BUG (e); }
            MyConnState conn = new MyConnState (sc);
            try { acore.register_selectable (sc, OP_READ, conn.read_cb); }
            catch (ClosedChannelException e) { BUG (e); }
        }
    };

    /////////////////////////////////////////////////////////////////
    //                                                             //
    //                    Connection-Tracking Code                 //
    //                                                             //
    /////////////////////////////////////////////////////////////////

    protected static class ProcInfo {
        public Constructor constructor;
        public Thunk2<Integer,XdrAble> handler;
        public ProcInfo (Class c, Thunk2<Integer,XdrAble> h) {
            assert XdrAble.class.isAssignableFrom (c);
            try { constructor = c.getConstructor (new Class [] {}); }
            catch (NoSuchMethodException e) { assert false; }
            handler = h;
        }
    }

    protected static final long CONN_TIMEOUT = 30*1000;
    protected class MyConnState {

        protected ByteBuffer write_buf = ByteBuffer.allocateDirect (16384);
        public SocketChannel sc;
        public InetAddress addr;
        public String client_string;
        public LinkedList<Function0<Boolean>> to_write = 
            new LinkedList<Function0<Boolean>> ();
        protected NioMultiplePacketInputBuffer ib =
            new NioMultiplePacketInputBuffer ();
        protected int next_read_size = -1;
        protected long last_activity_ms;
        protected int reqs_outstanding;

        public MyConnState (SocketChannel sc) {
            this.sc = sc;
            addr = sc.socket ().getInetAddress ();
            client_string = addr.getHostAddress() + ":" + sc.socket().getPort();
            last_activity_ms = timer_ms ();
            acore.register_timer (CONN_TIMEOUT, timeout_cb);

            // We write out the write_buf until position == limit.  Since
            // there's no response in here yet, we need to make sure we don't
            // write anything, so we set limit = position = 0.

            write_buf.limit (0);
        }

        protected Runnable timeout_cb = new Runnable () {
            public void run() {
                if (sc.isOpen ()) {
                    long now_ms = timer_ms ();
                    if (reqs_outstanding == 0 
                        && now_ms - last_activity_ms > CONN_TIMEOUT)
                        error_close_connection ("timeout");
                    else
                        acore.register_timer (CONN_TIMEOUT, timeout_cb);
                }
            }
        };

        protected Runnable read_cb = new Runnable () {
            public void run() {

                // Read all the packets we can off the wire.
                int count = 0;
                while (true) {
                    ByteBuffer read_buf = ByteBuffer.allocate(1500);
                    try { count = sc.read(read_buf); }
                    catch (IOException e) { conn_closed(); return; }
                    if (count < 0) { conn_closed(); return; }

                    // Sometimes, even though isReadable is true, nothing gets
                    // read.
                    if (count == 0) break;

                    if (logger.isDebugEnabled ()) 
                        logger.debug ("read packet of " + count + " bytes");

                    read_buf.flip ();
                    ib.add_packet (read_buf);
                }

                // Decode all of the requests we can.
                while (true) {

                    ib.unlimit ();
                    if (next_read_size == -1) {
                        if (ib.size() < 4) {
                            logger.debug ("less than 4 bytes available");
                            break;
                        }
                        next_read_size = ib.nextInt() & 0x7fffffff;
                    }

                    if (ib.size() < next_read_size) {
                        if (logger.isDebugEnabled ()) {
                            logger.debug ("need " + next_read_size + 
                                          " bytes, but only " + ib.size () +
                                          " bytes available");
                        }
                        break;
                    }

                    if (logger.isDebugEnabled ()) 
                        logger.debug ("req is " + next_read_size + " bytes");
                    int this_req_size = next_read_size;
                    if (this_req_size < 4) {
                        error_close_connection ("size < 4");
                        break;
                    }
                    next_read_size = -1;
                    ib.limit (this_req_size);
                    int xact_id = ib.nextInt();
                    try {
                        int msg_type = ib.nextInt();
                        int rpcvers = ib.nextInt();
                        int prog = ib.nextInt();
                        if (prog != 708655600) {
                            try { acore.unregister_selectable (sc, OP_READ); }
                            catch (ClosedChannelException e) { conn_closed (); }
                            send_resp (error_cb(xact_id, 1, "PROG_UNAVAIL")); 
                            break;
                        }
                        int vers = ib.nextInt();
                        if ((vers >= handlers.length) ||
                            (handlers[vers] == null)) {
                            logger.warn("unknown version " + vers);
                            try { acore.unregister_selectable (sc, OP_READ); }
                            catch (ClosedChannelException e) { conn_closed (); }
                            send_resp (mismatch_cb (xact_id)); 
                            break;
                        }
                        int proc = ib.nextInt();
                        if ((proc >= handlers[vers].length) ||
                            (handlers[vers][proc] == null)) {
                            logger.warn("unknown proc " + proc + ", version "
                                        + vers);
                            try { acore.unregister_selectable (sc, OP_READ); }
                            catch (ClosedChannelException e) { conn_closed (); }
                            send_resp (error_cb (xact_id, 3, "PROC_UNAVAIL")); 
                            break;
                        }

                        // the creditials and verifier
                        for (int i = 0; i < 2; ++i) {
                            int flavor = ib.nextInt();
                            int len = ib.nextInt();
                            for (int j = 0; j < len; ++j)
                                ib.nextByte();
                        }

                        XdrInputBufferDecodingStream ds =
                            new XdrInputBufferDecodingStream(ib,
                                    ib.limit_remaining ());
                        ds.beginDecoding ();
                        XdrAble xdr = (XdrAble) 
                            handlers[vers][proc].constructor.newInstance (
                                    new Object [] {});
                        xdr.xdrDecode (ds);
                        handlers[vers][proc].handler.run(
                                new Integer (xact_id), xdr);
                    }
                    catch (Error garbage) {
                        garbage.printStackTrace();
                        try { acore.unregister_selectable (sc, OP_READ); }
                        catch (ClosedChannelException e) { conn_closed (); }
                        send_resp (error_cb (xact_id, 4, "GARBAGE_ARGS"));
                        break;
                    }
                    catch (Exception garbage) {
                        garbage.printStackTrace();
                        try { acore.unregister_selectable (sc, OP_READ); }
                        catch (ClosedChannelException e) { conn_closed (); }
                        send_resp (error_cb (xact_id, 4, "GARBAGE_ARGS"));
                        break;
                    }
                }
            }
        };

        protected Runnable write_cb = new Runnable () {
            public void run() {
                while (! to_write.isEmpty ()) {
                    Function0<Boolean> cb = to_write.getFirst ();
                    if (cb.run().booleanValue ())
                        to_write.removeFirst ();
                    else
                        break;
                }
                if (to_write.isEmpty () && sc.isOpen ()) {
                    try { acore.unregister_selectable (sc, OP_WRITE); }
                    catch (ClosedChannelException e) { conn_closed (); }
                }
            }
        };

        protected Thunk1<String> close_cb = new Thunk1<String> () {
            public void run(String msg) { error_close_connection (msg); }
        };

        protected Function0<Boolean> error_cb(int xact_id, int code, String m) {
            return curry(normal_resp_cb, new Integer (xact_id), 
                         new Integer (code), new XdrVoid (), 
                         curry(close_cb, m));
        }

        protected Function0<Boolean> mismatch_cb (int xact_id) {
            XdrAble resp = new XdrAble () {
                public void xdrDecode (XdrDecodingStream xdr) {
                    throw new NoSuchMethodError ();
                }
                public void xdrEncode (XdrEncodingStream xdr) 
                throws OncRpcException, java.io.IOException {
                    xdr.xdrEncodeInt (2 /*low*/); xdr.xdrEncodeInt (2 /*high*/);
                }
            };
            return curry(normal_resp_cb, new Integer (xact_id), 
                         new Integer (2), resp,
                         curry(close_cb, "PROC_MISMATCH"));
        }

        protected Function4<Boolean,Integer,Integer,XdrAble,Runnable> 
            normal_resp_cb = 
            new Function4<Boolean,Integer,Integer,XdrAble,Runnable> () {
            public Boolean run(Integer xact_id, Integer code, XdrAble resp, 
                               Runnable done_cb) {
                if (write_buf.limit () == 0) {
                    write_buf.clear ();
                    write_buf.position (4); // reserve space for length
                    write_buf.putInt (xact_id.intValue ());
                    write_buf.putInt (1); // RPC_REPLY
                    write_buf.putInt (0); // MSG_ACCEPTED
                    write_buf.putInt (0); // AUTH_NULL
                    write_buf.putInt (0); // zero-length authorization
                    write_buf.putInt (code.intValue ());
                    XdrByteBufferEncodingStream es =
                        new XdrByteBufferEncodingStream (write_buf);
                    try { resp.xdrEncode (es); }
                    catch (Exception e) { assert false : e; }
                    write_buf.flip ();
                    write_buf.putInt ((write_buf.limit () - 4) | 0x80000000);
                    write_buf.position (0);
                }
                int n = 0;
                try { n = sc.write (write_buf); }
                catch (IOException e) { 
                    conn_closed (); 
                    return new Boolean (false);
                }
                if (write_buf.position () == write_buf.limit ()) {
                    write_buf.limit (0);
                    done_cb.run();
                    return new Boolean (true);
                }
                else {
                    return new Boolean (false);
                }
            }
        };

        protected void send_resp (Function0<Boolean> cb) {
            // If the connection has already been closed (presumably
            // because the client gave up waiting, then the skey will have
            // been cancelled, and so we don't want to call interestOps,
            // or it will throw an CancelledKeyException.
            if (sc.isOpen ()) {
                to_write.addLast (cb);
                try { acore.register_selectable (sc, OP_WRITE, write_cb); }
                catch (ClosedChannelException e) { conn_closed (); }
            }
        }

        protected Thunk2<Integer,XdrAble> handle_rpc_null_req = 
            new Thunk2<Integer,XdrAble> () {
            public void run(Integer xact_id, XdrAble xdr){
                last_activity_ms = timer_ms ();
                Runnable log_fn = new Runnable () { public void run() {} };
                send_resp (curry(normal_resp_cb, xact_id, 
                                 new Integer (0) /* SUCCESS */, 
                                 new XdrVoid (), log_fn));
            }
        };

        /////////////////////////////////////////////////////////////
        //
        //                    Version 2 Handlers
        //
        /////////////////////////////////////////////////////////////

        protected Thunk2<Integer,XdrAble> handle_rpc_put_req_2 = 
            new Thunk2<Integer,XdrAble> () {
            public void run(final Integer xact_id, XdrAble xdr){
                last_activity_ms = timer_ms ();
                bamboo_put_args put_args = (bamboo_put_args) xdr;
                final String client_lib = armor_string(put_args.client_library);
                final String application = armor_string (put_args.application);
                final Dht.PutReq outb = put_args_to_put_req (put_args, addr);
                md.update (outb.value.array (), outb.value.arrayOffset (), 
                        outb.value.limit ());
                final byte [] value_hash = md.digest ();
                final long now = timer_ms ();
                outb.user_data = new Thunk1<Dht.PutResp> () {
                    public void run(Dht.PutResp resp) {
                        rpc_put_done (outb, resp, xact_id, now, value_hash,
                                      client_lib, application);
                    }
                };
                log_put_req (xact_id, outb, client_string, value_hash,
                             client_lib, application);
                dispatch (outb);
                reqs_outstanding++;
            }
        };

        protected void rpc_put_done (Dht.PutReq req, Dht.PutResp resp, 
                                     Integer xact_id, long start_ms, 
                                     byte [] value_hash, String client_lib, 
                                     String application) {
            reqs_outstanding--;
            Runnable log_fn = curry(log_put_resp, xact_id, req, 
                                  resp, client_string, new Long (start_ms), 
                                  value_hash, client_lib, application);
            send_resp (curry(normal_resp_cb, xact_id,
                             new Integer (0) /* SUCCESS */, 
                             new XdrInt (resp.result), log_fn));
        }

        protected Thunk2<Integer,XdrAble> handle_rpc_get_req_2 = 
            new Thunk2<Integer,XdrAble> () {
            public void run(final Integer xact_id, XdrAble xdr){
                last_activity_ms = timer_ms ();
                bamboo_get_args get_args = (bamboo_get_args) xdr;
                final String client_lib = armor_string(get_args.client_library);
                final String application = armor_string (get_args.application);
                if ((get_args.placemark.value.length > 0) &&
                    (get_args.placemark.value.length != 
                     StorageManager.Key.SIZE)) {
                    assert ! sim_running;
                    error_close_connection("bad placemark size: " +
                            get_args.placemark.value.length);
                    return;
                }

                NodeId client = new NodeId(sc.socket().getPort(), addr);
                final Dht.GetReq outb = get_args_to_get_req (get_args, client);
                final long now = timer_ms ();
                outb.user_data = new Thunk1<Dht.GetResp> () {
                    public void run(Dht.GetResp resp) {
                        rpc_get_done_2(outb, resp, xact_id, now, client_lib,
                                       application);
                    }
                };
                log_get_req (xact_id, outb, client_string, client_lib, 
                             application);
                dispatch (outb);
                reqs_outstanding++;
            }
        };

        protected void rpc_get_done_2 (Dht.GetReq req, Dht.GetResp resp, 
                                     Integer xact_id, long start_ms,
                                     String client_lib, String application){
            reqs_outstanding--;
            Runnable log_fn = curry(log_get_resp, xact_id, req, 
                                  resp, client_string, new Long (start_ms), 
                                  client_lib, application);
            send_resp (curry(normal_resp_cb, xact_id, 
                             new Integer (0) /* SUCCESS */, 
                             get_resp_to_get_res (resp), log_fn));
        }

        /////////////////////////////////////////////////////////////
        //
        //                    Version 3 Handlers
        //
        /////////////////////////////////////////////////////////////

        protected Thunk2<Integer,XdrAble> handle_rpc_put_req_3 = 
            new Thunk2<Integer,XdrAble> () {
            public void run(final Integer xact_id, XdrAble xdr){
                last_activity_ms = timer_ms ();
                bamboo_put_arguments put_args = (bamboo_put_arguments) xdr;
                final String client_lib = armor_string(put_args.client_library);
                final String application = armor_string (put_args.application);
                final Dht.PutReq outb = put_args_to_put_req(put_args, addr);
                md.update (outb.value.array (), outb.value.arrayOffset (), 
                        outb.value.limit ());
                final byte [] value_hash = md.digest ();
                final long now = timer_ms ();
                outb.user_data = new Thunk1<Dht.PutResp> () {
                    public void run(Dht.PutResp resp) {
                        rpc_put_done (outb, resp, xact_id, now, value_hash,
                                      client_lib, application);
                    }
                };
                log_put_req (xact_id, outb, client_string, value_hash,
                             client_lib, application);
                dispatch (outb);
                reqs_outstanding++;
            }
        };

        protected Thunk2<Integer,XdrAble> handle_rpc_get_req_3 = 
            new Thunk2<Integer,XdrAble> () {
            public void run(final Integer xact_id, XdrAble xdr){
                last_activity_ms = timer_ms ();
                bamboo_get_args get_args = (bamboo_get_args) xdr;
                final String client_lib = armor_string(get_args.client_library);
                final String application = armor_string (get_args.application);
                if ((get_args.placemark.value.length > 0) &&
                    (get_args.placemark.value.length != 
                     StorageManager.Key.SIZE)) {
                    assert ! sim_running;
                    error_close_connection("bad placemark size: " +
                            get_args.placemark.value.length);
                    return;
                }
                NodeId client = new NodeId(sc.socket().getPort(), addr);
                final Dht.GetReq outb = get_args_to_get_req (get_args, client);
                final long now = timer_ms ();
                outb.user_data = new Thunk1<Dht.GetResp> () {
                    public void run(Dht.GetResp resp) {
                        rpc_get_done_3 (outb, resp, xact_id, now, client_lib,
                                      application);
                    }
                };
                log_get_req (xact_id, outb, client_string, client_lib, 
                             application);
                dispatch (outb);
                reqs_outstanding++;
            }
        };

        protected void rpc_get_done_3 (Dht.GetReq req, Dht.GetResp resp, 
                                     Integer xact_id, long start_ms,
                                     String client_lib, String application){
            reqs_outstanding--;
            Runnable log_fn = curry(log_get_resp, xact_id, req, 
                                  resp, client_string, new Long (start_ms), 
                                  client_lib, application);
            send_resp (curry(normal_resp_cb, xact_id, 
                             new Integer (0) /* SUCCESS */, 
                             get_resp_to_get_result (resp), log_fn));
        }

        protected Thunk2<Integer,XdrAble> handle_rpc_rm_req = 
            new Thunk2<Integer,XdrAble> () {
            public void run(final Integer xact_id, XdrAble xdr){
                last_activity_ms = timer_ms ();
                bamboo_rm_arguments rm_args = (bamboo_rm_arguments) xdr;
                final String client_lib = armor_string(rm_args.client_library);
                final String application = armor_string(rm_args.application);
                final Dht.PutReq outb = rm_args_to_put_req(rm_args, addr);
                final long now = timer_ms ();
                outb.user_data = new Thunk1<Dht.PutResp> () {
                    public void run(Dht.PutResp resp) {
                        rpc_rm_done (outb, resp, xact_id, now, client_lib, 
                                     application);
                    }
                };
                log_rm_req(xact_id.intValue(), outb, client_string, 
                           client_lib, application);
                dispatch (outb);
                reqs_outstanding++;
            }
        };

        protected void rpc_rm_done(Dht.PutReq req, Dht.PutResp resp, 
                                   Integer xact_id, long start_ms, 
                                   String client_lib, String application) {
            reqs_outstanding--;
            Runnable log_fn = curry(log_rm_resp, xact_id, req, resp, 
                                    client_string, new Long (start_ms), 
                                    client_lib, application);
            send_resp (curry(normal_resp_cb, xact_id,
                             new Integer (0) /* SUCCESS */, 
                             new XdrInt (resp.result), log_fn));
        }

        protected void conn_closed () {
            acore.unregister_selectable(sc);
            try { sc.close (); } catch (IOException e) {}
            if (logger.isInfoEnabled ()) {
                StringBuffer buf = new StringBuffer (45);
                buf.append ("connection closed by ");
                buf.append (client_string);
                logger.info (buf);
            }
        }

        protected void error_close_connection (String msg) {
            acore.unregister_selectable (sc);
            try { sc.close (); } catch (IOException e) {}
            if (logger.isInfoEnabled ()) {
                StringBuffer buf = new StringBuffer (45 + msg.length ());
                buf.append ("closing connection to ");
                buf.append (client_string);
                buf.append (": ");
                buf.append (msg);
                logger.info (buf);
            }
        }

        protected ProcInfo [][] handlers = {
            null, // no Version 0
            null, // no Version 1
            new ProcInfo[] { // Version 2
                null, // no Proc 0
                new ProcInfo (XdrVoid.class,         handle_rpc_null_req),
                new ProcInfo (bamboo_put_args.class, handle_rpc_put_req_2),
                new ProcInfo (bamboo_get_args.class, handle_rpc_get_req_2),
            },
            new ProcInfo[] { // Version 3
                null, // no Proc 0
                new ProcInfo (XdrVoid.class,              handle_rpc_null_req),
                new ProcInfo (bamboo_put_arguments.class, handle_rpc_put_req_3),
                new ProcInfo (bamboo_get_args.class,      handle_rpc_get_req_3),
                new ProcInfo (bamboo_rm_arguments.class,  handle_rpc_rm_req),
            },
        };
    }

    /////////////////////////////////////////////////////////////////
    //                                                             //
    //                    SandStorm Functions                      //
    //                                                             //
    /////////////////////////////////////////////////////////////////

    public void init (ConfigDataIF config) throws Exception {
        super.init (config);

        md = MessageDigest.getInstance ("SHA");
        allow_unauth_rm = config_get_boolean(config, "allow_unauth_rm");
        assert !allow_unauth_rm : "allow_unauth_rm no longer supported";
        server_port = config.getInt("port");
        if (sim_running) {
            if (instances == null) {
                instances = new HashMap ();
                simulator = Simulator.instance();
                bbuf = ByteBuffer.allocate(16384);
            }
            // Client will look us up by the RPC port, not the UdpCC one.
            NodeId gid = new NodeId (server_port, my_node_id.address ());
            instances.put(gid, this);
            logger.info ("gid=" + gid);
        }
        else {
            ssock_channel = ServerSocketChannel.open();
            ssock = ssock_channel.socket();
            ssock.bind(new InetSocketAddress(server_port));
            ssock_channel.configureBlocking(false);
            acore.register_selectable (ssock_channel, OP_ACCEPT, accept_cb);
        }

        acore.register_timer (0, new Runnable () { public void run() {
                WebInterface www = WebInterface.instance (my_node_id);
                if (www == null) {
                    logger.warn("no WebInterface stage; XML RPC disabled");
                }
                else {
                    // Version 2 equivalents in XML RPC:
                    www.register_xml_rpc_handler("put", new XmlRpcPutHandler());
                    www.register_xml_rpc_handler("get", new XmlRpcGetHandler());

                    // Version 3 equivalents in XML RPC:
                    www.register_xml_rpc_handler(
                        "put_removable", new XmlRpcPutRemovableHandler());
                    www.register_xml_rpc_handler(
                        "get_details", new XmlRpcGetHandler());
                    www.register_xml_rpc_handler("rm", new XmlRpcRmHandler());
                }
            }});
    }

    public void handleEvent (QueueElementIF item) {
	if (logger.isDebugEnabled ()) logger.debug ("got " + item);
        if (item instanceof Dht.PutResp) {
            Dht.PutResp resp = (Dht.PutResp) item;
            Thunk1<Dht.PutResp> cb = (Thunk1<Dht.PutResp>) resp.user_data;
            cb.run(resp);
        }
        else if (item instanceof Dht.GetResp) {
            Dht.GetResp resp = (Dht.GetResp) item;
            Thunk1<Dht.GetResp> cb = (Thunk1<Dht.GetResp>) resp.user_data;
            cb.run(resp);
        }
        else {
            BUG("unexpected event: " + item);
        }
    }

    /////////////////////////////////////////////////////////////////
    //                                                             //
    //                  Translation Functions                      //
    //                                                             //
    /////////////////////////////////////////////////////////////////

    public BigInteger byte_array_to_big_int (byte [] origb) {
        if ((origb [0] & 0x80) == 0)
            return new BigInteger (origb);
        else {
            byte [] keyb = new byte [origb.length+1];
            System.arraycopy (origb, 0, keyb, 1, origb.length);
            return new BigInteger (keyb);
        }
    }

    protected int ttl(int requested) {
        return Math.min(requested, Dht.MAX_TTL_SEC);
    }

    protected Dht.PutReq put_args_to_put_req (
            bamboo_put_args put_args, InetAddress client) {
        return new Dht.PutReq(byte_array_to_big_int(put_args.key.value), 
                              ByteBuffer.wrap(put_args.value.value), null,
                              true /* put */, my_sink, null,
                              ttl(put_args.ttl_sec), client);
    }

    protected Dht.PutReq put_args_to_put_req (
            bamboo_put_arguments put_args, InetAddress client) {
        byte[] secret_hash = null;
        if (put_args.secret_hash.hash.length > 0)
            secret_hash = put_args.secret_hash.hash;
        // TODO: check hash type
        return new Dht.PutReq(byte_array_to_big_int(put_args.key.value), 
                              ByteBuffer.wrap(put_args.value.value), 
                              secret_hash, null, true /* put */, my_sink, null,
                              ttl(put_args.ttl_sec), client);
    }

    protected Dht.PutReq rm_args_to_put_req(bamboo_rm_arguments rm_args,
                                            InetAddress client) {
        BigInteger key = byte_array_to_big_int (rm_args.key.value);
        int ttl_sec = (rm_args.ttl_sec > Dht.MAX_TTL_SEC) 
            ? Dht.MAX_TTL_SEC : rm_args.ttl_sec;
        byte [] secret_hash = md.digest(rm_args.secret);
        // TODO: check hash type
        return new Dht.PutReq(key, ByteBuffer.wrap(rm_args.secret), 
                              secret_hash, rm_args.value_hash.hash, 
                              false /* remove */, 
                              my_sink, null, ttl_sec, client);
    }

    protected Dht.GetReq get_args_to_get_req (bamboo_get_args get_args,
                                              NodeId client) {
        BigInteger key = byte_array_to_big_int(get_args.key.value);
        StorageManager.Key placemark = null;
        if (get_args.placemark.value.length > 0) {
            placemark = new StorageManager.Key(
                ByteBuffer.wrap(get_args.placemark.value));
        }
        // HACK: if maxvals is MAX_VALUE - 1, we set "all" to false, which
        // indicates to the Dht stage that it's okay to use the new sync
        // technique.
        boolean all = get_args.maxvals != Integer.MAX_VALUE - 1;
        return new Dht.GetReq(key, get_args.maxvals,
                              all, placemark, my_sink, null, client);
    }

    protected bamboo_get_res get_resp_to_get_res (Dht.GetResp resp) {

        bamboo_get_res gresp = new bamboo_get_res();
        gresp.values = new bamboo_value[resp.values.size()];

        int j = 0;
        Iterator i = resp.values.iterator();
        while (i.hasNext()) {
            Object obj = i.next();
            ByteBuffer buf = null;
            if (obj instanceof GetValue) 
                buf = ((GetValue) obj).value;
            else 
                buf = (ByteBuffer) obj;
            assert buf.position() == 0;
            assert buf.hasArray();
            assert buf.arrayOffset() == 0;
            assert buf.array().length == buf.limit();
            gresp.values[j] = new bamboo_value();
            gresp.values[j].value = buf.array();
            ++j;
        }
        gresp.placemark = new bamboo_placemark();
        if (resp.placemark.equals (StorageManager.ZERO_KEY)) {
            gresp.placemark.value = new byte [0];
        }
        else {
            gresp.placemark.value =
                new byte[StorageManager.Key.SIZE];
            resp.placemark.to_byte_buffer(
                    ByteBuffer.wrap(gresp.placemark.value));
        }

        return gresp;
    }

    protected bamboo_get_result get_resp_to_get_result(Dht.GetResp resp) {

        bamboo_get_result gresp = new bamboo_get_result();
        gresp.values = new bamboo_get_value[resp.values.size()];

        int j = 0;
        Iterator i = resp.values.iterator();
        while (i.hasNext()) {
            Object obj = i.next();
            GetValue gv = null;
            if (obj instanceof GetValue)
                gv = (GetValue) obj;
            else {
                ByteBuffer value = (ByteBuffer) obj;
                gv = new GetValue(value, -1, new byte[0]);
            }
            assert gv.value.position() == 0;
            assert gv.value.hasArray();
            assert gv.value.arrayOffset() == 0;
            assert gv.value.array().length == gv.value.limit();
            gresp.values[j] = new bamboo_get_value();
            gresp.values[j].value = new bamboo_value();
            gresp.values[j].value.value = gv.value.array();
            gresp.values[j].ttl_sec_rem = gv.ttlRemaining;
            gresp.values[j].secret_hash = new bamboo_hash();
            gresp.values[j].secret_hash.algorithm = gv.hashAlgorithm;
            gresp.values[j].secret_hash.hash = gv.secretHash;
            ++j;
        }
        gresp.placemark = new bamboo_placemark();
        if (resp.placemark.equals (StorageManager.ZERO_KEY)) {
            gresp.placemark.value = new byte [0];
        }
        else {
            gresp.placemark.value =
                new byte[StorageManager.Key.SIZE];
            resp.placemark.to_byte_buffer(
                    ByteBuffer.wrap(gresp.placemark.value));
        }

        return gresp;
    }

    /////////////////////////////////////////////////////////////////
    //                                                             //
    //                    XML RPC Interface                        //
    //                                                             //
    /////////////////////////////////////////////////////////////////

    public class XmlRpcPutHandler {
        public void put (byte [] key_bytes, byte [] value, int ttl_sec,
                String application, String user_agent, 
                InetSocketAddress client, Thunk1<Object> result_cb) {

            if (key_bytes.length > 20) {
                result_cb.run(new Exception ("key is longer than 20 bytes"));
                return;
            }
            if (value.length > 1024) {
                result_cb.run(
                        new Exception ("value is longer than 1024 bytes"));
                return;
            }

            BigInteger key = byte_array_to_big_int (key_bytes);
            ttl_sec = (ttl_sec > Dht.MAX_TTL_SEC) ? Dht.MAX_TTL_SEC : ttl_sec;

            String client_string = client.getAddress ().getHostAddress () + 
                    ":" + client.getPort ();

            Long start_ms = new Long (now_ms ());

            md.update (value, 0, value.length);
            byte [] value_hash = md.digest ();

            Dht.PutReq req = new Dht.PutReq(
                    key, ByteBuffer.wrap(value), null, true /* put */, 
                    my_sink, null, ttl_sec, client.getAddress());
            req.user_data = curry(xml_rpc_put_done, result_cb, req, 
                                  client_string, start_ms, value_hash, 
                                  user_agent, application); 

            log_put_req (-1, req, client_string, value_hash, user_agent, 
                         application);

            dispatch (req);
        }
    }

    public class XmlRpcPutRemovableHandler {
        public void put_removable (byte [] key_bytes, byte [] value, 
                String hash_alg, byte [] secret_hash, int ttl_sec, 
                String application, String user_agent, 
                InetSocketAddress client, Thunk1<Object> result_cb) {

            if (key_bytes.length > 20) {
                result_cb.run(new Exception ("key is longer than 20 bytes"));
                return;
            }
            if (value.length > 1024) {
                result_cb.run(
                        new Exception ("value is longer than 1024 bytes"));
                return;
            }
            if (!hash_alg.equals("SHA")) {
                result_cb.run(new Exception ("unknown secret hash algorithm: \""
                            + hash_alg + "\""));
                return;
            }
            if (secret_hash.length > 20) {
                result_cb.run(
                        new Exception ("secret hash is longer than 20 bytes"));
                return;
            }

            BigInteger key = byte_array_to_big_int (key_bytes);
            ttl_sec = (ttl_sec > Dht.MAX_TTL_SEC) ? Dht.MAX_TTL_SEC : ttl_sec;

            String client_string = client.getAddress ().getHostAddress () + 
                    ":" + client.getPort ();

            Long start_ms = new Long (now_ms ());

            md.update (value, 0, value.length);
            byte [] value_hash = md.digest ();

            Dht.PutReq req = new Dht.PutReq(
                    key, ByteBuffer.wrap(value), secret_hash, null, 
                    true /* put */, my_sink, null, ttl_sec, 
                    client.getAddress());
            req.user_data = curry(xml_rpc_put_done, result_cb, req, 
                                  client_string, start_ms, value_hash, 
                                  user_agent, application); 

            log_put_req (-1, req, client_string, value_hash, user_agent, 
                         application);

            dispatch (req);
        }
    }

    public Thunk8<Thunk1<Object>,Dht.PutReq,String,Long,byte[],String,String,
        Dht.PutResp> xml_rpc_put_done = 
        new Thunk8<Thunk1<Object>,Dht.PutReq,String,Long,byte[],String,String,
        Dht.PutResp> () {

        public void run(Thunk1<Object> result_cb, Dht.PutReq req, 
                          String client, Long start_ms, byte [] value_hash,
                          String client_lib, String application,
                          Dht.PutResp resp) {

            log_put_resp.run(new Integer (-1), req, resp, client, start_ms, 
                             value_hash, client_lib, application);

            result_cb.run(new Integer (resp.result));
        }
    };

    public class XmlRpcRmHandler {
        public void rm(byte [] key_bytes, byte [] value_hash, 
                String hash_alg, byte [] secret, int ttl_sec, 
                String application, String user_agent, 
                InetSocketAddress client, Thunk1<Object> result_cb) {

            if (key_bytes.length > 20) {
                result_cb.run(new Exception("key is longer than 20 bytes"));
                return;
            }
            if (value_hash.length > 20) {
                result_cb.run(
                        new Exception("value_hash is longer than 20 bytes"));
                return;
            }
            if (!hash_alg.equals("SHA")) {
                result_cb.run(new Exception ("unknown secret hash algorithm: \""
                            + hash_alg + "\""));
                return;
            }
            if (secret.length > 40) {
                result_cb.run(new Exception ("secret is longer than 40 bytes"));
                return;
            }

            BigInteger key = byte_array_to_big_int(key_bytes);
            ttl_sec = (ttl_sec > Dht.MAX_TTL_SEC) ? Dht.MAX_TTL_SEC : ttl_sec;

            String client_string = client.getAddress().getHostAddress() + 
                    ":" + client.getPort();

            Long start_ms = new Long(now_ms());

            byte [] secret_hash = md.digest(secret);
            Dht.PutReq req = new Dht.PutReq(key, ByteBuffer.wrap(secret), 
                    secret_hash, value_hash, false /* remove */, my_sink, 
                    null, ttl_sec, client.getAddress());
            /* Dht.PutReq req = new Dht.PutReq(
                    key, ByteBuffer.wrap(new byte[0]), value_hash, 
                    false, my_sink, null, ttl_sec, 
                    client.getAddress()); */
            req.user_data = curry(xml_rpc_rm_done, result_cb, req, 
                                  client_string, start_ms, user_agent, 
                                  application); 

            log_rm_req(-1, req, client_string, user_agent, application);

            dispatch(req);
        }
    }

    public Thunk7<Thunk1<Object>,Dht.PutReq,String,Long,String,String,
        Dht.PutResp> xml_rpc_rm_done = 
        new Thunk7<Thunk1<Object>,Dht.PutReq,String,Long,String,String,
        Dht.PutResp> () {

        public void run(Thunk1<Object> result_cb, Dht.PutReq req, 
                          String client, Long start_ms, String client_lib, 
                          String application, Dht.PutResp resp) {

            log_rm_resp.run(new Integer(-1), req, resp, client, start_ms, 
                            client_lib, application);

            result_cb.run(new Integer(resp.result));
        }
    };

    public class XmlRpcGetHandler {
        private void get_common(byte [] key_bytes, int maxvals, 
                                byte [] placemark_bytes, String application, 
                                String user_agent, InetSocketAddress client, 
                                Thunk1<Object> result_cb, boolean details) {

            if (key_bytes.length > 20) {
                result_cb.run(new Exception ("key is longer than 20 bytes"));
                return;
            }
            if (placemark_bytes.length > StorageManager.Key.SIZE) {
                result_cb.run(new Exception ("placemark is longer than " + 
                            StorageManager.Key.SIZE + " bytes"));
                return;
            }

            BigInteger key = byte_array_to_big_int(key_bytes);
            StorageManager.Key placemark = null;
            if (placemark_bytes.length > 0) {
                placemark = new StorageManager.Key (
                        ByteBuffer.wrap(placemark_bytes));
            }

            String client_string = client.getAddress ().getHostAddress () + 
                    ":" + client.getPort ();

            Long start_ms = new Long (now_ms ());

            // HACK: if maxvals is MAX_VALUE - 1, we set "all" to false, which
            // indicates to the Dht stage that it's okay to use the new sync
            // technique.
            boolean all = maxvals != Integer.MAX_VALUE - 1;
            Dht.GetReq req = new Dht.GetReq (key, maxvals, all,
                                             placemark, my_sink, null,
                                             NodeId.create(client));
            req.user_data = curry(xml_rpc_get_done, result_cb, req, 
                                  client_string, start_ms, user_agent, 
                                  application, details);

            log_get_req (-1, req, client_string, user_agent, application);

            dispatch (req);
        }

        public void get(byte [] key_bytes, int maxvals, 
                        byte [] placemark_bytes, String application, 
                        String user_agent, InetSocketAddress client, 
                        Thunk1<Object> result_cb) {
            get_common(key_bytes, maxvals, placemark_bytes, application, 
                       user_agent, client, result_cb, false);
        }

        public void get_details(byte [] key_bytes, int maxvals, 
                                byte [] placemark_bytes, String application, 
                                String user_agent, InetSocketAddress client, 
                                Thunk1<Object> result_cb) {
            get_common(key_bytes, maxvals, placemark_bytes, application, 
                       user_agent, client, result_cb, true);
        }
    }

    Thunk8<Thunk1<Object>,Dht.GetReq,String,Long,String,String,
           Boolean,Dht.GetResp> xml_rpc_get_done = new 
    Thunk8<Thunk1<Object>,Dht.GetReq,String,Long,String,String,
           Boolean,Dht.GetResp> () {
        public void run(Thunk1<Object> result_cb, Dht.GetReq req, 
                        String client, Long start_ms, String client_lib, 
                        String application, Boolean details, Dht.GetResp resp) {

            log_get_resp.run(new Integer (-1), req, resp, client, 
                             start_ms, client_lib, application);

            Vector result = new Vector (2);

            Vector values = new Vector (resp.values.size ());
            if (details.booleanValue()) {
                for (Object object : resp.values) {
                    GetValue gv = (GetValue) object;
                    assert gv.value.position() == 0;
                    assert gv.value.hasArray();
                    assert gv.value.arrayOffset() == 0;
                    assert gv.value.array().length == gv.value.limit();
                    if (details) {
                        Vector value = new Vector(4);
                        value.add(gv.value.array());
                        value.add(gv.ttlRemaining);
                        value.add(gv.hashAlgorithm);
                        value.add(gv.secretHash);
                        values.add(value);
                    }
                    else {
                        values.add(gv.value.array());
                    }
                }
            }
            else {
                for (Object object : resp.values) {
                    ByteBuffer buf = ((GetValue) object).value;
                    assert buf.position() == 0;
                    assert buf.hasArray();
                    assert buf.arrayOffset() == 0;
                    assert buf.array().length == buf.limit();
                    values.add (buf.array ());
                }
            }
            result.add (values);

            byte [] placemark_bytes = null;
            if (resp.placemark.equals (StorageManager.ZERO_KEY)) {
                placemark_bytes = new byte [0];
            }
            else {
                placemark_bytes = new byte [StorageManager.Key.SIZE];
                resp.placemark.to_byte_buffer(ByteBuffer.wrap(placemark_bytes));
            }
            result.add (placemark_bytes);

            result_cb.run(result);
        }
    };

    /////////////////////////////////////////////////////////////////
    //                                                             //
    //                    Logging Functions                        //
    //                                                             //
    /////////////////////////////////////////////////////////////////

    protected String armor_string (String input) {
        byte [] in_bytes = input.getBytes ();
        byte [] out_bytes = new byte [in_bytes.length * 2];
        int j = 0;
        for (int i = 0; i < in_bytes.length; ++i) {
            if ((in_bytes [i] < 0x20) || (in_bytes [i] > 0x7e)) {
                // no control codes
                out_bytes [j++] = '.';
            }
            else if (in_bytes [i] == '"') {
                // escape quotes
                out_bytes [j++] = '\\'; 
                out_bytes [j++] = '"'; 
            }
            else {
                // pass everything else
                out_bytes [j++] = in_bytes [i];
            }
        }
        return new String (out_bytes, 0, j);
    }

    protected MessageDigest md;
    protected void log_put_req (int xact_id, Dht.PutReq req, String client,
                                 byte [] value_hash,
                                 String client_lib, String application) {
        if (logger.isInfoEnabled ()) {
            StringBuffer buf = new StringBuffer (100);
            buf.append ("put req client=");
            buf.append (client);
            buf.append (" client_library=\"");
            buf.append (client_lib);
            buf.append ("\" application=\"");
            buf.append (application);
            buf.append ("\" xact_id=0x");
            buf.append (Integer.toHexString (xact_id));
            buf.append (" key=0x");
            buf.append (GuidTools.guid_to_string (req.key));
            buf.append (" secret_hash=0x");
            if (req.secret_hash == null)
                bytes_to_sbuf (StorageManager.ZERO_HASH, 0, 4, false, buf);
            else
                bytes_to_sbuf(req.secret_hash, 0, 
                              min(req.secret_hash.length, 4), false, buf);
            buf.append (" value_hash=0x");
            bytes_to_sbuf(value_hash, 0, min(value_hash.length, 4), false, buf);
            buf.append (" size=");
            buf.append (req.value.limit ());
            buf.append (" ttl=");
            buf.append (req.ttl_sec);
            logger.info (buf);
        }
    }

    protected Thunk8<Integer,Dht.PutReq,Dht.PutResp,String,Long,
                            byte[],String,String> log_put_resp = 
            new Thunk8<Integer,Dht.PutReq,Dht.PutResp,String,Long,
                       byte[],String,String> () {

        public void run(Integer xact_id, Dht.PutReq req, Dht.PutResp resp,
                        String client, Long start_ms, 
                        byte [] value_hash,
                        String client_lib, String application) {

            if (logger.isInfoEnabled()) {
                StringBuffer buf = new StringBuffer(200);
                buf.append ("put resp client=");
                buf.append (client);
                buf.append (" client_library=\"");
                buf.append (client_lib);
                buf.append ("\" application=\"");
                buf.append (application);
                buf.append ("\" xact_id=0x");
                buf.append (Integer.toHexString(xact_id));
                buf.append (" key=0x");
                buf.append (GuidTools.guid_to_string (req.key));
                buf.append (" value hash=0x");
                if (req.secret_hash == null)
                    bytes_to_sbuf (StorageManager.ZERO_HASH, 0, 4, false, buf);
                else
                    bytes_to_sbuf(req.secret_hash, 0, 
                                  min(req.secret_hash.length, 4), false, buf);
                buf.append (" value hash=0x");
                bytes_to_sbuf(value_hash, 0, min(value_hash.length, 4), 
                              false, buf);
                buf.append (" size=");
                buf.append (req.value.limit ());
                buf.append (" ttl=");
                buf.append (req.ttl_sec);
                buf.append (" stat=");
                buf.append (resp.result);
                buf.append (" lat=");
                buf.append (timer_ms () - start_ms.longValue ());
                buf.append (" ms");
                logger.info(buf);
            }
        }
    };

    protected void log_rm_req(int xact_id, Dht.PutReq req, String client,
                              String client_lib, String application) {
        if (logger.isInfoEnabled ()) {
            StringBuffer buf = new StringBuffer (100);
            buf.append ("rm req client=");
            buf.append (client);
            buf.append (" xact_id=0x");
            buf.append (Integer.toHexString (xact_id));
            buf.append (" key=0x");
            buf.append (GuidTools.guid_to_string (req.key));
            buf.append (" secret_hash=0x");
            if (req.secret_hash == null)
                bytes_to_sbuf (StorageManager.ZERO_HASH, 0, 4, false, buf);
            else
                bytes_to_sbuf(req.secret_hash, 0, 
                              min(req.secret_hash.length, 4), false, buf);
            buf.append (" value_hash=0x");
            bytes_to_sbuf(req.value_hash, 0, min(req.value_hash.length, 4), 
                          false, buf);
            buf.append (" secret=0x");
            assert req.value.hasArray();
            bytes_to_sbuf(req.value.array(), 
                          req.value.arrayOffset() + req.value.position(), 
                          req.value.limit() - req.value.position(), 
                          false, buf);
            buf.append (" ttl=");
            buf.append (req.ttl_sec);
            buf.append ("s");
            logger.info (buf);
        }
    }

    protected Thunk7<Integer,Dht.PutReq,Dht.PutResp,String,Long,
                            String,String> log_rm_resp = 
            new Thunk7<Integer,Dht.PutReq,Dht.PutResp,String,Long,
                       String,String> () {

        public void run(Integer xact_id, Dht.PutReq req, Dht.PutResp resp,
                        String client, Long start_ms, String client_lib, 
                        String application) {

            if (logger.isInfoEnabled()) {
                StringBuffer buf = new StringBuffer(200);
                buf.append ("rm resp client=");
                buf.append (client);
                buf.append (" client_library=\"");
                buf.append (client_lib);
                buf.append ("\" application=\"");
                buf.append (application);
                buf.append ("\" xact_id=0x");
                buf.append (Integer.toHexString(xact_id));
                buf.append (" key=0x");
                buf.append (GuidTools.guid_to_string (req.key));
                buf.append (" secret_hash=0x");
                if (req.secret_hash == null)
                    bytes_to_sbuf (StorageManager.ZERO_HASH, 0, 4, false, buf);
                else
                    bytes_to_sbuf(req.secret_hash, 0, 
                                  min(req.secret_hash.length, 4), false, buf);
                buf.append (" value_hash=0x");
                bytes_to_sbuf(req.value_hash, 0, min(req.value_hash.length, 4), 
                              false, buf);
                buf.append (" secret=0x");
                assert req.value.hasArray();
                bytes_to_sbuf(req.value.array(), 
                              req.value.arrayOffset() + req.value.position(), 
                              req.value.limit() - req.value.position(), 
                              false, buf);
                buf.append (" ttl=");
                buf.append (req.ttl_sec);
                buf.append (" stat=");
                buf.append (resp.result);
                buf.append (" lat=");
                buf.append (timer_ms () - start_ms.longValue ());
                buf.append (" ms");
                logger.info(buf);
            }
        }
    };


    protected void log_get_req (int xact_id, Dht.GetReq req, String client,
            String client_lib, String application) {
        if (logger.isInfoEnabled()) {
            StringBuffer buf = new StringBuffer(200);
            buf.append("get req client=");
            buf.append(client);
            buf.append (" client_library=\"");
            buf.append (client_lib);
            buf.append ("\" application=\"");
            buf.append (application);
            buf.append("\" xact_id=0x");
            buf.append(Integer.toHexString(xact_id));
            buf.append(" key=0x");
            buf.append(GuidTools.guid_to_string(req.key));
            buf.append(" maxvals=");
            buf.append(req.maxvals);
            buf.append(" placemark=(");
            if ((req.placemark == null) || req.placemark.equals(ZERO_KEY))
                buf.append("NONE");
            else
                buf.append(req.placemark);
            buf.append(")");
            logger.info(buf);
        }
    }

    protected Thunk7<Integer,Dht.GetReq,Dht.GetResp,String,Long,String,String>
        log_get_resp = new 
        Thunk7<Integer,Dht.GetReq,Dht.GetResp,String,Long,String,String> () {
      
        public void run(Integer xact_id, Dht.GetReq req, Dht.GetResp resp,
                        String client, Long start_ms, 
                        String client_lib, String application) {

            if (logger.isInfoEnabled()) {
                StringBuffer buf = new StringBuffer(400);
                buf.append("get resp client=");
                buf.append(client);
                buf.append (" client_library=\"");
                buf.append (client_lib);
                buf.append ("\" application=\"");
                buf.append (application);
                buf.append("\" xact_id=0x");
                buf.append(Integer.toHexString(xact_id.intValue ()));
                buf.append(" key=0x");
                buf.append(GuidTools.guid_to_string(req.key));
                buf.append(" maxvals=");
                buf.append(req.maxvals);
                buf.append(" req placemark=(");
                if ((req.placemark == null) || req.placemark.equals(ZERO_KEY))
                    buf.append("NONE");
                else
                    buf.append(req.placemark);
                buf.append(") values=[");
                Iterator i = resp.values.iterator ();
                while (i.hasNext ()) {
                    Object obj = i.next();
                    GetValue gv = null;
                    if (obj instanceof GetValue)
                        gv = (GetValue) obj;
                    else {
                        ByteBuffer value = (ByteBuffer) obj;
                        gv = new GetValue(value, -1, new byte[0]);
                    }
                    md.update (gv.value.array (), gv.value.arrayOffset (),
                               gv.value.limit ());
                    byte [] value_hash = md.digest ();
                    buf.append ("(");
                    buf.append (gv.value.limit ());
                    if (gv.secretHash.length > 0) {
                        buf.append (", ");
                        buf.append (gv.hashAlgorithm);
                        buf.append(" 0x");
                        bytes_to_sbuf(gv.secretHash, 0,
                                      min(gv.secretHash.length, 4), false, buf);
                    }
                    else 
                        buf.append (", NONE");
                    buf.append (", 0x");
                    bytes_to_sbuf(value_hash, 0, 
                                  min(value_hash.length, 4), false, buf);
                    buf.append (", ");
                    buf.append (gv.ttlRemaining);
                    buf.append (")");
                    if (i.hasNext ())
                        buf.append (", ");
                }
                buf.append("] resp placemark=(");
                if ((resp.placemark == null) || resp.placemark.equals(ZERO_KEY))
                    buf.append("NONE");
                else
                    buf.append(resp.placemark);
                buf.append (") lat=");
                buf.append (timer_ms () - start_ms.longValue ());
                buf.append (" ms");
                logger.info(buf);
            }
        }
    };

    /////////////////////////////////////////////////////////////////
    //                                                             //
    //                    Simulator Code                           //
    //                                                             //
    /////////////////////////////////////////////////////////////////

    public static Map instances;

    public void do_put (final int xact_id, bamboo_put_args args, 
                        final NodeId client_id, final GatewayClient client) {
        final Dht.PutReq outb = put_args_to_put_req (args, client_id.address());
        md.update (outb.value.array (), outb.value.arrayOffset (), 
                   outb.value.limit ());
        final byte [] value_hash = md.digest ();
        final long now = timer_ms ();
        outb.user_data = new Thunk1<Dht.PutResp> () {
            public void run(Dht.PutResp resp) {
                simulator_put_done (outb, resp, client, client_id, xact_id, 
                                    now, value_hash);
            }
        }; 
        log_put_req(xact_id, outb, client.toString(), value_hash, "sim", "sim");
        dispatch (outb);
    }

    /* TODO
    public void do_rm(final int xact_id, bamboo_rm_args args, 
                      final NodeId client_id, final GatewayClient client) {
        final Dht.PutReq outb = rm_args_to_put_req(args, client_id.address());
        final long now = timer_ms();
        outb.user_data = new Thunk1<Dht.PutResp>() {
            public void run(Dht.PutResp resp) {
                simulator_rm_done(outb, resp, client, client_id, xact_id, now);
            }
        }; 
        log_rm_req(xact_id, outb, client.toString(), "sim", "sim");
        dispatch(outb);
    }
    */

    public void do_get (final int xact_id, bamboo_get_args args, 
                        final NodeId client_id, final GatewayClient client) {
        final Dht.GetReq outb = get_args_to_get_req (args, client_id);
        final long now = timer_ms ();
        outb.user_data = new Thunk1<Dht.GetResp> () {
            public void run(Dht.GetResp resp) {
                simulator_get_done(outb, resp, client, client_id, xact_id, now);
            }
        };
        log_get_req (xact_id, outb, client.toString(), "sim", "sim");
        dispatch (outb);
    }

    protected static Simulator simulator;
    protected static ByteBuffer bbuf;

    public NodeId my_node_id () {
        return my_node_id;
    }

    protected void simulator_put_done (Dht.PutReq req, Dht.PutResp resp, 
                                       final GatewayClient client,
                                       NodeId client_id, final int xact_id,
                                       final long start_ms, 
                                       byte [] value_hash) {
        log_put_resp.run(new Integer (xact_id), req, resp, 
                         client_id.toString (), new Long (start_ms),
                         value_hash, "sim", "sim");
        int size = 4;
        long lat = Network.msg_latency_us(my_node_id, client_id, size) / 1000;
        // TODO: what if the Gateway dies before it receives this?
        simulator.event_queue.register_timer(client_id, lat, 
                new EventQueue.Callback() {
                    public void call(Object not_used) {
                        client.put_done (xact_id, 0 /* BAMBOO_OK */);
                    }
                }, null);
    }

    protected void simulator_rm_done(Dht.PutReq req, Dht.PutResp resp, 
                                     final GatewayClient client,
                                     NodeId client_id, final int xact_id,
                                     final long start_ms) {
        log_rm_resp.run(new Integer(xact_id), req, resp, client_id.toString(), 
                        new Long(start_ms), "sim", "sim");
        int size = 4;
        long lat = Network.msg_latency_us(my_node_id, client_id, size) / 1000;
        // TODO: what if the Gateway dies before it receives this?
        simulator.event_queue.register_timer(client_id, lat, 
                new EventQueue.Callback() {
                    public void call(Object not_used) {
                        client.rm_done(xact_id, 0 /* BAMBOO_OK */);
                    }
                }, null);
    }

    protected void simulator_get_done (Dht.GetReq req, Dht.GetResp resp, 
                                       final GatewayClient client, 
                                       NodeId client_id, final int xact_id,
                                       final long start_ms) {
        log_get_resp.run(xact_id, req, resp, client_id.toString (), start_ms,
                      "sim", "sim");
        bamboo_get_res res = get_resp_to_get_res (resp);
        final bamboo_get_res clone =
            (bamboo_get_res) XdrClone.xdr_clone (res, bbuf);
        int size = bbuf.position();
        long lat = Network.msg_latency_us(my_node_id, client_id, size) / 1000;
        // TODO: what if the Gateway dies before it receives this?
        simulator.event_queue.register_timer(client_id, lat, 
                new EventQueue.Callback () {
                    public void call(Object not_used) {
                        client.get_done(xact_id, clone);
                    }
                }, null);
    }
}

