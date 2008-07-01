/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.openhash.multicast;
import bamboo.lss.ASyncCore;
import bamboo.lss.NioMultiplePacketInputBuffer;
import bamboo.lss.NioOutputBuffer;
import java.io.IOException;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.LinkedList;
import org.apache.log4j.Logger;
import ostore.util.CountBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;
import ostore.util.QSInt;             // used only in test code
import static bamboo.util.Curry.*;
import static java.nio.channels.SelectionKey.*;

public class TcpMsgChannel {

    protected static final Logger logger =
        Logger.getLogger(TcpMsgChannel.class);

    protected ASyncCore acore;
    protected int nextReadSize = -1;
    protected ByteBuffer outbuf;
    protected InetSocketAddress peer;
    protected SocketChannel channel;
    protected Thunk1<QuickSerializable> recv;
    protected Runnable fail;
    protected LinkedList<QuickSerializable> outq =
        new LinkedList<QuickSerializable>();
    protected NioMultiplePacketInputBuffer ib = 
        new NioMultiplePacketInputBuffer();

    public TcpMsgChannel(ASyncCore a, InetSocketAddress peer) {
        acore = a;
        this.peer = peer;
        try { 
            channel = SocketChannel.open();
            channel.configureBlocking(false); 
        }
        catch (IOException e) { assert false; }
        try { channel.socket().setKeepAlive(true); }
        catch (SocketException e) {}
        try { 
            channel.connect(peer); 
            acore.registerSelectable(channel, OP_CONNECT, connect);
        }
        catch (IOException e) { assert false; }
    }

    /**
     * Build with an already-connected channel.
     */
    public TcpMsgChannel(ASyncCore a, SocketChannel c) {
        if (c == null) 
            throw new NullPointerException();
        acore = a;
        channel = c;
        peer = (InetSocketAddress) channel.socket().getRemoteSocketAddress();
        try { channel.socket().setKeepAlive(true); }
        catch (SocketException e) {}
    }

    public InetSocketAddress peer() { return peer; }

    public void send(QuickSerializable msg) {
        if (msg == null) 
            throw new NullPointerException();
        if (channel == null) 
            throw new IllegalStateException();
        else {
            try { acore.registerSelectable(channel, OP_WRITE, write); }
            catch (ClosedChannelException e) { error(); }
            outq.addLast(msg);
        }
    }

    public void setReceiveHandler(Thunk1<QuickSerializable> value) {
        if (recv != null) 
            throw new IllegalStateException();
        recv = value;
        if ((channel != null) && channel.isConnected()) {
            try { acore.registerSelectable(channel, OP_READ, read); }
            catch (ClosedChannelException e) { error(); }
        }
    }

    public void setFailureHandler(Runnable value) {
        if (fail != null) 
            throw new IllegalStateException();
        fail = value;
        if (channel == null) 
            fail.run();
    }

    public void close() {
        if (channel != null) {
            try { 
                acore.unregisterSelectable(channel);
                channel.close(); 
            } 
            catch (IOException e) {}
            channel = null;
        }
    }
                     
    protected void error() {
        close();
        if (fail != null) fail.run();
    }

    protected Runnable connect = new Runnable() {
        public void run() {
            try { 
                if (!channel.finishConnect()) 
                    return; 
                acore.unregisterSelectable(channel, OP_CONNECT);
            }
            catch (IOException e) { error(); return; }
            
            if (recv != null) {
                try { acore.registerSelectable(channel, OP_READ, read); }
                catch (ClosedChannelException e) { error(); }
            }
        }
    };

    protected Runnable read = new Runnable() {
        public void run() {

            // Read all the packets we can off the wire.
            while (true) {
                ByteBuffer inbuf = ByteBuffer.allocate(1500);
                int count = 0;
                try { count = channel.read(inbuf); }
                catch (IOException e) { error(); return; }
                if (count < 0) { error(); return; }
                if (count == 0) break;
                inbuf.flip();
                ib.add_packet(inbuf);
            }

            // Decode all of the requests we can.
            while (true) {
                ib.unlimit();
                if (nextReadSize == -1) {
                    if (ib.size() < 4)
                        break;
                    nextReadSize = ib.nextInt() & 0x7fffffff;
                }
                if (ib.size() < nextReadSize)
                    break;
                ib.limit(nextReadSize);
                try { recv.run(ib.nextObject()); }
                catch (QSException e) { 
                    logger.warn("Unknown type " + e + ".  Closing connection");
                    error(); return; 
                }
                if (ib.limit_remaining() != 0) { error(); return; }
                nextReadSize = -1;
            }
        }
    };

    protected Runnable write = new Runnable() {
        public void run() {
            while (true) {
                if (outbuf == null) {
                    if (outq.isEmpty()) {
                        try { acore.unregisterSelectable(channel, OP_WRITE); }
                        catch (ClosedChannelException e) { error(); }
                        break;
                    }
                    QuickSerializable qs = outq.removeFirst();
                    CountBuffer cb = new CountBuffer();
                    cb.add(qs);
                    outbuf = ByteBuffer.allocate(cb.size() + 4);
                    NioOutputBuffer ob = new NioOutputBuffer(outbuf);
                    ob.add(cb.size());
                    ob.add(qs);
                    outbuf.flip();
                }

                int n = 0;
                try { n = channel.write(outbuf); }
                catch (IOException e) { error(); }
                if (outbuf.position() == outbuf.limit())
                    outbuf = null;
                else
                    break;
            }
        }
    };

    public static void main(String args[]) throws Exception {
        ostore.util.TypeTable.register_type(QSInt.class);
        final ASyncCore acore = new bamboo.lss.ASyncCoreImpl();
        InetAddress addr = InetAddress.getByName(args[0]);
        int port = Integer.parseInt(args[1]);
        InetSocketAddress peer = new InetSocketAddress(addr, port);
        Runnable fail = new Runnable() {
            public void run() {
                System.err.println("connection failed");
                System.exit(1);
            }
        };
        Thunk1<QuickSerializable> recv = new Thunk1<QuickSerializable>() {
            public void run(QuickSerializable qs) {
                System.out.println("received: " + qs);
            }
        };
        final TcpMsgChannel c = new TcpMsgChannel(acore, peer);
        c.setReceiveHandler(recv);
        c.setFailureHandler(fail);
        c.send(new QSInt(-1));
        Thunk1<Integer> periodic = new Thunk1<Integer>() {
            public void run(Integer i) {
                if (i.intValue() == 10) {
                    c.close();
                    return;
                }
                c.send(new QSInt(i.intValue()));
                acore.registerTimer(1000, 
                        curry(this, new Integer(i.intValue() + 1)));
            }
        };
        acore.registerTimer(0, curry(periodic, new Integer(0)));
        acore.asyncMain();
    }
}

