/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.transport;
import java.io.IOException;
import java.net.UnknownHostException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import org.apache.log4j.Level;
import ostore.util.CountBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import bamboo.lss.ASyncCore;
import bamboo.lss.DustDevil;
import bamboo.lss.NioMultiplePacketInputBuffer;
import bamboo.lss.NioOutputBuffer;

/**
 * A message transport over TCP using the OceanStore serialization code.
 *
 * @author Sean C. Rhea
 * @version $Id: TcpMessageTransport.java,v 1.3 2004/03/09 21:17:53 srhea Exp $
 */
public class TcpMessageTransport extends bamboo.util.StandardStage 
implements SingleThreadedEventHandlerIF {

    public interface SendDoneCb {
        void send_done_cb (Object user_data);
    }

    public void send (InetSocketAddress dst, QuickSerializable payload,
            SendDoneCb cb, Object user_data) {

        if (logger.isDebugEnabled ()) {
            logger.debug ("got " + payload + " to send to " 
                    + dst.getAddress ().getHostAddress () + ":" 
                    + dst.getPort ());
        }

        ChannelState state = (ChannelState) client_channels.get (dst);
        if (state == null) {
            if (logger.isDebugEnabled ())
                logger.debug ("no existing connection");
            state = new ChannelState ();
            client_channels.put (dst, state);
            try { 
                state.channel = SocketChannel.open (); 
                state.channel.configureBlocking (false);
                state.channel.connect (dst);
                state.skey = acore.register_selectable (state.channel, 
                        SelectionKey.OP_READ | SelectionKey.OP_CONNECT,
                        state, null);
            }
            catch (Exception e) { assert false; }
            state.client = true;
        }
        else {
            if (logger.isDebugEnabled ())
                logger.debug ("existing connection");
            state.skey.interestOps (
                    state.skey.interestOps () | SelectionKey.OP_WRITE);
        }

        state.waiting.addLast (new SendState (payload, cb, user_data));
        if (logger.isDebugEnabled ())
            logger.debug ("queue size=" + state.waiting.size ());
    }

    public interface RecvMsgCb {
        void recv_msg_cb (InetSocketAddress src, QuickSerializable payload);
    }

    public void register_recv_cb (Class msg_type, RecvMsgCb cb) {
        if (recv_cbs.containsKey (msg_type))
            throw new IllegalStateException (msg_type.getName ());
        else
            recv_cbs.put (msg_type, cb);
    }

    protected ServerSocketChannel ssock_channel;
    protected Map client_channels = new HashMap ();
    protected Map server_channels = new HashMap ();
    protected Map recv_cbs = new HashMap ();
    protected InetSocketAddress my_addr;

    protected class ServerSocketCb implements ASyncCore.SelectableCB {
        public void select_cb (SelectionKey skey, Object user_data) {
            if (skey.isAcceptable ()) {
                SocketChannel sc = null;
                try { 
		    sc = ssock_channel.accept (); 
		}
		catch (IOException e) { 
                    BUG (e);
		}
		// Sometimes, even though isAcceptable is true, there still
		// isn't a connection to accept.
		if (sc == null) return;

                Socket s = sc.socket ();
                if (logger.isInfoEnabled ()) {
                    StringBuffer buf = new StringBuffer (45);
                    buf.append ("got connection from ");
                    buf.append (s.getInetAddress ().getHostAddress ());
                    buf.append (":");
                    buf.append (s.getPort ());
                    logger.info (buf);
                }
                try { 
		    sc.configureBlocking (false);
		}
		catch (IOException e) { 
                    BUG (e);
		}
                ChannelState state = new ChannelState ();
                state.channel = sc;
                try {
                    state.skey = acore.register_selectable (
                            state.channel, SelectionKey.OP_READ, state, null);
                }
                catch (ClosedChannelException e) { 
                    BUG (e);
		}

                InetSocketAddress addr = 
                    new InetSocketAddress (s.getInetAddress (), s.getPort ());
                server_channels.put (addr, state);
            }
        }
    }

    public class SendState {
        public QuickSerializable payload;
        public SendDoneCb cb;
        public Object user_data;
        public SendState (QuickSerializable p, SendDoneCb c, Object u) {
            payload = p; cb = c; user_data = u;
        }
    }

    public class ChannelState implements ASyncCore.SelectableCB {
        public SocketChannel channel;
        public ByteBuffer write_buf;
        public LinkedList waiting = new LinkedList ();
        public LinkedList inflight = new LinkedList ();
        public int acks_to_send;
        public NioMultiplePacketInputBuffer ib = 
            new NioMultiplePacketInputBuffer ();
        public int next_read_size = -1;
        public boolean client;
        public SelectionKey skey;

        public void select_cb (SelectionKey skey, Object user_data) {
            ChannelState state = (ChannelState) user_data;

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
                catch (Exception e) { BUG (e); }
            }

            if ((skey.readyOps () & skey.interestOps () 
                 & SelectionKey.OP_WRITE) != 0) {

                logger.debug ("op_write");
                while (true) {
                    if ((write_buf != null) 
                        && (write_buf.position () < write_buf.limit ())) {

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
                        catch (IOException e) { BUG (e); }
                        if (write_buf.position () < write_buf.limit ())
                            break;

                        if (logger.isDebugEnabled ()) {
                            logger.debug (
                                    (write_buf.limit () -write_buf.position ()) 
                                    + " bytes remaining");
                        }
                    }
                    if (waiting.isEmpty () && (acks_to_send == 0)) {
                        skey.interestOps (skey.interestOps () 
                                & ~SelectionKey.OP_WRITE);
                        break;
                    }

                    if (! waiting.isEmpty ()) {
                        assert client;

                        SendState ss = (SendState) waiting.removeFirst ();
                        CountBuffer cb = new CountBuffer ();
                        cb.add (ss.payload);
                        int msg_size = cb.size () 
                            + 4  // length
                            + 4  // IP addr
                            + 2; // port

                        if ((write_buf == null) 
                                || (write_buf.capacity () < msg_size)) {
                            write_buf = ByteBuffer.allocate (msg_size);
                        }
                        else {
                            write_buf.clear ();
                        }

                        write_buf.putInt (msg_size-4);
                        write_buf.put (
                                my_addr.getAddress ().getAddress (), 0, 4);
                        write_buf.putShort ((short) my_addr.getPort ());

                        NioOutputBuffer ob = new NioOutputBuffer (write_buf);
                        ob.add (ss.payload);
                        write_buf.flip ();

                        inflight.addLast (ss);
                    }

                    if (acks_to_send > 0) {
                        assert ! client;

                        // Send a byte for each ack

                        if ((write_buf == null) 
                                || (write_buf.capacity () < acks_to_send)) {
                            write_buf = ByteBuffer.allocate (acks_to_send);
                        }
                        else {
                            write_buf.clear ();
                        }
                        for (int i = 0; i < acks_to_send; ++i) 
                            write_buf.put ((byte) 0xbe);
                        write_buf.flip ();

                        acks_to_send = 0;
                    }
                }
            }

            if ((skey.readyOps () & skey.interestOps () 
                 & SelectionKey.OP_READ) != 0) {

                logger.debug ("op_read");

                if (client) {
                    logger.debug ("client");
                    while (true) {
                        ByteBuffer read_buf = ByteBuffer.allocate (10);
                        int count = 0;
                        try { count = channel.read (read_buf); }
                        catch (IOException e) { BUG (e); } // TODO
                        assert count >= 0;                 // TODO
                        if (count == 0) break;

                        if (logger.isDebugEnabled ())
                            logger.debug ("count=" + count);
                        for (int i = 0; i < count; ++i) {
                            assert ! inflight.isEmpty ();  // TODO
                            SendState ss = (SendState) inflight.removeFirst ();
                            ss.cb.send_done_cb (ss.user_data);
                        }
                    }
                }
                else {
                    logger.debug ("server");
                    while (true) {
                        ByteBuffer read_buf = ByteBuffer.allocate (1500);
                        int count = 0;
                        try { count = channel.read (read_buf); }
                        catch (IOException e) { BUG (e); }
                        assert count >= 0;
                        if (count > 0) {
                            read_buf.flip ();
                            ib.add_packet (read_buf);
                        }

                        if (logger.isDebugEnabled ())
                            logger.debug ("next_read_size=" + next_read_size
                                    + " ib.size=" + ib.size ());

                        if (next_read_size == -1) {
                            if (ib.size () < 4)
                                break;
                            next_read_size = ib.nextInt ();
                        }

                        if (logger.isDebugEnabled ())
                            logger.debug ("next_read_size=" + next_read_size
                                    + " ib.size=" + ib.size ());

                        if (ib.size () < next_read_size) 
                            break;

                        byte [] addrb = new byte [4];
                        ib.nextBytes (addrb, 0, 4);

                        InetAddress addr = null;
                        try { addr = InetAddress.getByAddress (addrb); }
                        catch (UnknownHostException e) { assert false : e; }

                        int port = ib.nextShort () & 0xffff;

                        InetSocketAddress src = 
                            new InetSocketAddress (addr, port);

                        QuickSerializable payload = null;
                        try { payload = ib.nextObject (); }
                        catch (Exception e) { assert false; } // TODO

                        RecvMsgCb cb = (RecvMsgCb) 
                            recv_cbs.get (payload.getClass ());

                        if (logger.isDebugEnabled ())
                            logger.debug ("received " + payload + " from "
                                    + src.getAddress ().getHostAddress () 
                                    + ":" + src.getPort ());

                        if (cb == null) {
                            if (logger.isDebugEnabled ())
                                logger.debug ("no recv handler for type " 
                                        + payload.getClass ().getName ());
                        }
                        else {
                            cb.recv_msg_cb (src, payload);
                        }

                        ++acks_to_send;
                        skey.interestOps (skey.interestOps () |
                                SelectionKey.OP_WRITE);

                        next_read_size = -1;
                    }
                }
            }
        }
    }

    public void init (ConfigDataIF config) throws Exception {
	super.init (config);
        int debug_level = config_get_int (config, "debug_level");
        if (debug_level > 0) 
            logger.setLevel (Level.DEBUG);
    	int port = config_get_int (config, "port");
        if (port == -1) 
            port = my_node_id.port () + 3;
        my_addr = new InetSocketAddress (my_node_id.address (), port);
	ssock_channel = ServerSocketChannel.open ();
        ServerSocket ssock = ssock_channel.socket ();
	ssock.bind (new InetSocketAddress (port));
        ssock_channel.configureBlocking (false);
        acore.register_selectable (ssock_channel, SelectionKey.OP_ACCEPT, 
                new ServerSocketCb (), null);
    }

    public void handleEvent (QueueElementIF item) {
        BUG ("unexpected event: " + item);
    }
}

