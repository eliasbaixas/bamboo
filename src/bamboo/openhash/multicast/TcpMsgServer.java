/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.openhash.multicast;
import bamboo.lss.ASyncCore;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import ostore.util.QSInt;             // used only in test code
import ostore.util.QuickSerializable; // used only in test code
import static bamboo.util.Curry.*;
import static java.nio.channels.SelectionKey.*;

public class TcpMsgServer {
                        
    protected ASyncCore acore;
    protected Thunk1<TcpMsgChannel> cb;
    protected ServerSocketChannel channel;

    public TcpMsgServer(ASyncCore acore, int port, Thunk1<TcpMsgChannel> cb) 
    throws IOException {
        this.acore = acore;
        this.cb = cb;
        channel = ServerSocketChannel.open();
        channel.socket().bind(new InetSocketAddress(port));
        channel.configureBlocking(false);
        acore.register_selectable(channel, OP_ACCEPT, accept_cb);
    }

    protected Runnable accept_cb = new Runnable() {
        public void run() {
            SocketChannel sc = null;
            try { sc = channel.accept(); }
            catch (IOException e) { assert false; }
            // Sometimes, even though isAcceptable is true, there still
            // isn't a connection to accept.
            if (sc == null) return;
            try { sc.configureBlocking(false); }
            catch (IOException e) { assert false; }
            cb.run(new TcpMsgChannel(acore, sc));
        }
    };

    public void close() {
        try { channel.close(); } catch (IOException e) {}
    }

    public static void main(String args[]) throws Exception {
        ostore.util.TypeTable.register_type(QSInt.class);
        final ASyncCore acore = new bamboo.lss.ASyncCoreImpl();
        int port = Integer.parseInt(args[0]);
        final Thunk1<InetSocketAddress> fail = new Thunk1<InetSocketAddress>() {
            public void run(InetSocketAddress addr) {
                System.err.println("connection to " + addr + " failed");
                System.exit(1);
            }
        };
        final Thunk2<TcpMsgChannel,QuickSerializable> recv = 
            new Thunk2<TcpMsgChannel,QuickSerializable>() {
            public void run(TcpMsgChannel channel, QuickSerializable qs) {
                System.out.println("received " + qs + " from " +channel.peer());
                channel.send(new QSInt(0));
            }
        };
        Thunk1<TcpMsgChannel> cb = new Thunk1<TcpMsgChannel>() {
            public void run(TcpMsgChannel channel) {
                channel.setReceiveHandler(curry(recv, channel));
                channel.setFailureHandler(curry(fail, channel.peer()));
            }
        };
        TcpMsgServer server = new TcpMsgServer(acore, port, cb);
        acore.asyncMain();
    }
}

