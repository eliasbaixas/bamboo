import bamboo.lss.*;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.LinkedList;
import static bamboo.util.Curry.*;
import static java.nio.channels.SelectionKey.*;

public class EchoServer {

    public static ASyncCore acore;

    public static Thunk1<ServerSocketChannel> acceptCallback = 
            new Thunk1<ServerSocketChannel>() {
        public void run(ServerSocketChannel serverChannel) {
            try { 
                SocketChannel channel = serverChannel.accept(); 
                if (channel != null) {
                    channel.configureBlocking(false); 
                    LinkedList<ByteBuffer> bufs = 
                        new LinkedList<ByteBuffer>();
                    acore.registerSelectable(channel, OP_READ, 
                            curry(readCallback,channel,bufs)); 
                }
            }
            catch (IOException e) {
                System.err.println("Could not accept: " + e);
                System.exit(1);
            }
        }
    };

    public static Thunk2<SocketChannel,LinkedList<ByteBuffer>> readCallback = 
            new Thunk2<SocketChannel,LinkedList<ByteBuffer>>() {
        public void run(SocketChannel channel, LinkedList<ByteBuffer> bufs) {
            while (true) {
                ByteBuffer buf = ByteBuffer.wrap(new byte[1024]);
                try {
                    int n = channel.read(buf);
                    if (n > 0) {
                        buf.flip();
                        bufs.addLast(buf);
                        acore.registerSelectable(channel, OP_WRITE,
                                curry(writeCallback, channel, bufs));
                    }
                    else {
                        if (n < 0) 
                            throw new IOException("channel closed");
                        break;
                    }
                }
                catch (IOException e1) {
                    try { 
                        channel.close(); 
                        acore.unregisterSelectable(channel, 
                                                   OP_READ | OP_WRITE);
                    } 
                    catch (IOException e2) { /* Do nothing. */ }
                }
            }
        }
    };

    public static Thunk2<SocketChannel,LinkedList<ByteBuffer>> writeCallback = 
            new Thunk2<SocketChannel,LinkedList<ByteBuffer>>() {
        public void run(SocketChannel channel, LinkedList<ByteBuffer> bufs) {
            try {
                while (! bufs.isEmpty()) {
                    ByteBuffer buf = bufs.getFirst();
                    while (buf.position() < buf.limit()) {
                        if (channel.write(buf) == 0)
                            return;
                    }
                    if (buf.position() == buf.limit())
                        bufs.removeFirst();
                }
                acore.unregisterSelectable(channel, OP_WRITE);
            }
            catch (IOException e) {
                System.err.println("Could not write: " + e);
                System.exit(1);
            }
        }
    };

    public static void main(String [] args) throws IOException {
        if (args.length < 1) {
            System.err.println("usage: java EchoServer <port>");
            System.exit(1);
        }
        int port = Integer.parseInt(args[0]);
        acore = new ASyncCoreImpl();
        ServerSocketChannel channel = ServerSocketChannel.open();
        channel.socket().bind(new InetSocketAddress(port));
        channel.configureBlocking(false);
        acore.registerSelectable(channel, OP_ACCEPT, 
                curry(acceptCallback, channel));
        acore.asyncMain();
    }
}
