/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.openhash.i3;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.LinkedList;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import bamboo.lss.ASyncCore;
import java.nio.channels.DatagramChannel;
import bamboo.openhash.redir.RedirClient;
import java.security.MessageDigest;
import java.math.BigInteger;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import ostore.util.ByteUtils;
import bamboo.util.GuidTools;
import java.io.IOException;
import bamboo.lss.UdpCC;
import ostore.util.QuickSerializable;
import ostore.util.CountBuffer;
import bamboo.lss.NioOutputBuffer;
import bamboo.lss.NioInputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.InputBuffer;
import ostore.util.QSException;
import ostore.util.TypeTable;

/**
 * Used to replace the i3 chord_server with a ReDiR-based lookup service.
 *
 * @author Sean C. Rhea
 * @version $Id: LookupService.java,v 1.6 2004/11/20 18:06:14 srhea Exp $
 */
public class LookupService extends bamboo.util.StandardStage
implements SingleThreadedEventHandlerIF {

    protected static final String APP = 
        "bamboo.openhash.i3.LookupService $Revision: 1.6 $";

    public static class I3Msg implements QuickSerializable {
        public BigInteger dest;
        public byte [] payload_bytes;
        public I3Msg (BigInteger k, byte [] p) {
            dest = k; payload_bytes = p;
        }
        public I3Msg (InputBuffer ib) throws QSException {
            dest = ib.nextBigInteger ();
            int len = ib.nextInt ();
            payload_bytes = new byte [len];
            ib.nextBytes (payload_bytes, 0, len);
        }
        public void serialize (OutputBuffer ob) {
            ob.add (dest);
            ob.add (payload_bytes.length);
            ob.add (payload_bytes);
        }
    }

    public static class PredMsg implements QuickSerializable {
        public BigInteger key;
        public InetSocketAddress addr;
        public PredMsg (BigInteger k, InetSocketAddress a) {
            key = k; addr = a;
        }
        public PredMsg (InputBuffer ib) throws QSException {
            key = ib.nextBigInteger ();
            byte [] ip_bytes = new byte [6];
            ib.nextBytes (ip_bytes, 0, 6);
            addr = RedirClient.bytes2addr(ip_bytes);
        }
        public void serialize (OutputBuffer ob) {
            ob.add (key);
            byte [] ip_bytes = RedirClient.addr2bytes(addr);
            ob.add (ip_bytes);
        }
    }

    protected DatagramChannel channel;
    protected ByteBuffer read_buf = ByteBuffer.allocate (16384);
    protected LinkedList waiting = new LinkedList ();
    protected SelectionKey skey;
    protected RedirClient client;
    protected BigInteger namespace;
    protected BigInteger my_id;
    protected BigInteger succ_id;
    protected BigInteger pred_id;
    protected int levels;
    protected InetSocketAddress my_addr;
    protected InetSocketAddress i3_addr;
    protected UdpCC udpcc;

    protected ASyncCore.SelectableCB select_cb = new ASyncCore.SelectableCB (){
        public void select_cb (SelectionKey skey, Object user_data) {
            logger.debug ("select_cb");
            if ((skey.readyOps () & skey.interestOps ()
                 & SelectionKey.OP_WRITE) != 0) {
                logger.debug ("op_write");
                while (true) {

                    if (waiting.isEmpty ()) {
                        skey.interestOps(
                            skey.interestOps() & ~SelectionKey.OP_WRITE);
                        break;
                    }

                    ByteBuffer pkt =  (ByteBuffer) waiting.getFirst ();
                    int n = 0;
                    try { n = channel.send (pkt, i3_addr); }
                    catch (IOException e) { BUG (e); }
                    if (n == 0)
                        break;

                    waiting.removeFirst ();
                    assert pkt.position() == pkt.limit ();

                    if (logger.isDebugEnabled ()) {
                        pkt.flip ();
                        logger.debug ("sent bytes:\n" + ByteUtils.print_bytes (
                                    pkt.array (), pkt.arrayOffset (),
                                    pkt.limit () - pkt.position ()));
                    }
                }
            }

            if ((skey.readyOps () & skey.interestOps ()
                 & SelectionKey.OP_READ) != 0) {
                logger.debug ("op_read");
                while (true) {
                    read_buf.clear ();
                    InetSocketAddress src = null;
                    try { src = (InetSocketAddress) channel.receive(read_buf); }
                    catch (IOException e) { BUG (e); }
                    if (src == null)
                        break;

                    read_buf.flip ();
                    handle_inb_pkt (src, read_buf);
                }
            }
        }
    };

    public void handle_inb_pkt (InetSocketAddress src, ByteBuffer pkt) {
        if (logger.isDebugEnabled()) {
            logger.debug("Got inbound packet " + ByteUtils.print_bytes (
                    pkt.array(),  pkt.arrayOffset(),  pkt.limit()));
        }
        byte first_byte = pkt.get();
        if (first_byte == 0x0) {
            // Data packet
            byte [] dest_bytes = new byte [20];
            pkt.get(dest_bytes);
            BigInteger dest = RedirClient.bytes2bi(dest_bytes);
            int payload_len = pkt.getShort();
            byte [] payload_bytes = new byte [payload_len];
            pkt.get(payload_bytes);
            if (logger.isDebugEnabled()) {
                logger.debug ("got msg to send to 0x"
                              + GuidTools.guid_to_string (dest)
                              + "; doing lookup");
            }
            client.lookup(dest, namespace, levels, APP, lookup_cb, 
                          payload_bytes);
        }
        else if (first_byte == (byte) 0xff) {
            // i3 hullo
            logger.debug("got i3 hullo");
            i3_addr = src;
            if (succ_id != null)
                send_i3_range (); // otherwise we'll send it when we learn it
        }
        else {
            assert false : first_byte;
        }
    }

    public void init (ConfigDataIF config) throws Exception {
        super.init(config);

        TypeTable.register_type (I3Msg.class);
        TypeTable.register_type (PredMsg.class);

        my_addr = new InetSocketAddress(
            my_node_id.address(), my_node_id.port());
        logger.debug ("my_node_id=" + my_node_id);
        logger.debug ("my_addr=" + my_addr);
        byte [] my_addr_bytes = RedirClient.addr2bytes(my_addr);
        MessageDigest digest = MessageDigest.getInstance("SHA");
        my_id = RedirClient.bytes2bi(digest.digest(my_addr_bytes));
        logger.info("my_id=0x" + GuidTools.guid_to_string(my_id));
        levels = config_get_int (config, "levels");
        String namespace_str = config_get_string(config, "namespace");
        namespace = RedirClient.bytes2bi(
            digest.digest(namespace_str.getBytes()));
        String client_stg_name =
            config_get_string(config, "client_stage_name");
        client = (RedirClient) lookup_stage(config, client_stg_name);
        channel = DatagramChannel.open();
        DatagramSocket sock = channel.socket();
        int port = config_get_int (config, "i3_port");
        InetSocketAddress addr = new InetSocketAddress (port);
        sock.bind(addr);
        channel.configureBlocking(false);
        skey = acore.register_selectable(channel, SelectionKey.OP_READ,
                                         select_cb, null);
        acore.register_timer (0, ready_cb, null);
        udpcc = new UdpCC (acore, my_addr, serializer, sink);
    }

    protected UdpCC.Serializer serializer = new UdpCC.Serializer () {
        public int serialize_size (Object msg) {
            QuickSerializable qs = (QuickSerializable) msg;
            CountBuffer cb = new CountBuffer ();
            cb.add (qs);
            return cb.size ();
        }
        public void serialize (Object msg, ByteBuffer buf) {
            QuickSerializable qs = (QuickSerializable) msg;
            NioOutputBuffer ob = new NioOutputBuffer (buf);
            ob.add (qs);
        }
        public Object deserialize (ByteBuffer buf) throws Exception {
            NioInputBuffer ib = new NioInputBuffer (buf);
            return ib.nextObject ();
        }
    };

    protected UdpCC.Sink sink = new UdpCC.Sink () {
        public void recv (Object msg, InetSocketAddress src,
                          InetSocketAddress local, int tries, long wait_ms,
                          long est_rtt_ms) {
            if (msg instanceof I3Msg) {
                I3Msg inb = (I3Msg) msg;
                ByteBuffer pkt = ByteBuffer.wrap (inb.payload_bytes);
                if (logger.isDebugEnabled()) {
                    logger.debug ("got i3 message for 0x"
                                  + GuidTools.guid_to_string(inb.dest) + ":\n"
                                  + ByteUtils.print_bytes(inb.payload_bytes));
                }
                send_pkt(pkt);
            }
            else if (msg instanceof PredMsg) {
                PredMsg inb = (PredMsg) msg;
                if (logger.isDebugEnabled()) {
                    logger.debug ("got pred message from 0x"
                                  + GuidTools.guid_to_string(inb.key));
                }
                pred_id = inb.key;
                send_i3_range (); // even though it may not have changed
            }
            else {
                logger.warn("got unknown msg type: "+msg.getClass().getName());
            }
        }
    };

    public ASyncCore.TimerCB ready_cb = new ASyncCore.TimerCB () {
        public void timer_cb(Object user_data) {
            logger.debug("joining");
            client.join(my_addr, my_id, namespace, levels, 3600, 
                        APP, join_cb, null);
        }
    };

    public RedirClient.JoinCb join_cb = new RedirClient.JoinCb () {
        public void join_cb (Object user_data) {
            logger.info ("joined");
            stabilize_cb.timer_cb(null);
        }
    };

    public RedirClient.LookupCb found_succ_cb = new RedirClient.LookupCb () {
        public void lookup_cb (BigInteger key, InetSocketAddress succ_addr,
                               BigInteger succ_hash, int gets, 
                               Object user_data) {
            if (logger.isDebugEnabled()) {
                logger.debug("found successor: 0x"
                             + GuidTools.guid_to_string(succ_hash));
            }
            succ_id = succ_hash;
            PredMsg msg = new PredMsg (my_id, my_addr);
            udpcc.send (msg, succ_addr, 10, pred_send_cb, msg);
            acore.register_timer(10*1000, stabilize_cb, null);
        }
    };

    protected UdpCC.SendCB pred_send_cb = new UdpCC.SendCB () {
        public void cb(Object user_data, boolean success) {
            if (! success) {
                logger.warn ("PredMsg failed");
            }
        }
    };

    protected ASyncCore.TimerCB stabilize_cb = new ASyncCore.TimerCB () {
        public void timer_cb (Object user_data) {
            logger.debug ("stabilizing");
            client.lookup(my_id, namespace, levels, APP, found_succ_cb, null);
        }
    };

    protected void send_i3_range () {
        if (i3_addr == null)
            return;

        BigInteger high = my_id.subtract(BigInteger.ONE);
        if (logger.isDebugEnabled()) {
            logger.debug("sending range to i3: [" + pred_id.toString(16)
                         + ", " + high.toString (16) + "]");
        }

        ByteBuffer pkt = ByteBuffer.allocate (41);
        pkt.put ((byte) 0x02);
        pkt.put (RedirClient.bi2bytes(pred_id));
        pkt.put (RedirClient.bi2bytes(high));
        pkt.flip ();
        send_pkt (pkt);
    }

    protected void send_pkt (ByteBuffer pkt) {
        waiting.addLast(pkt);
        skey.interestOps(skey.interestOps() | SelectionKey.OP_WRITE);
    }

    public RedirClient.LookupCb lookup_cb = new RedirClient.LookupCb () {
        public void lookup_cb (BigInteger key, InetSocketAddress succ_addr,
                               BigInteger succ_hash, int gets, 
                               Object user_data) {
            byte [] payload_bytes = (byte []) user_data;
            if (logger.isDebugEnabled()) {
                logger.debug("lookup for 0x" + GuidTools.guid_to_string(key)
                             + " returned "
                             + GuidTools.guid_to_string(succ_hash));
            }
            I3Msg outb = new I3Msg (key, payload_bytes);
            udpcc.send (outb, succ_addr, 10, i3_send_cb, outb);
        }
    };

    protected UdpCC.SendCB i3_send_cb = new UdpCC.SendCB () {
        public void cb (Object user_data, boolean success) {
            if (!success) {
                // repeat lookup and try again
                I3Msg msg = (I3Msg) user_data;
                logger.info("send to key 0x"
                            + GuidTools.guid_to_string(msg.dest)
                            + " failed, trying lookup again");
                client.lookup(msg.dest, namespace, levels, APP, lookup_cb,
                              msg.payload_bytes);
            }
        }
    };

    public void handleEvent (QueueElementIF item) {
        BUG ("unexpected event: " + item);
    }
}
