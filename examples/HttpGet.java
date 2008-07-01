import bamboo.lss.*;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import static bamboo.util.Curry.*;
import static java.nio.channels.SelectionKey.*;

public class HttpGet {

    public static ASyncCore acore;

    public static Thunk1<SocketChannel> acceptCallback = 
            new Thunk1<SocketChannel>() {
        public void run(SocketChannel channel) {
            try {
                if (channel.finishConnect()) {
                   acore.unregisterSelectable(channel, OP_CONNECT);
                   byte [] bytes = ("GET / HTTP/1.0\r\n\r\n").getBytes();
                   ByteBuffer buf = ByteBuffer.wrap(bytes);
                   acore.registerSelectable(channel, OP_WRITE, 
                           curry(writeCallback, channel, buf));
                }
            }
            catch (IOException e) {
                System.err.println("Could not connect: " + e);
                System.exit(1);
            }
        }
    };

    public static Thunk2<SocketChannel,ByteBuffer> writeCallback = 
            new Thunk2<SocketChannel,ByteBuffer>() {
        public void run(SocketChannel channel, ByteBuffer buf) {
            try {
                while (buf.position() < buf.limit()) {
                    if (channel.write(buf) == 0)
                        break;
                }
                if (buf.position() == buf.limit()) {
                    acore.unregisterSelectable(channel, OP_WRITE);
                    acore.registerSelectable(channel, OP_READ, 
                            curry(readCallback, channel));
                }
            }
            catch (IOException e) {
                System.err.println("Could not write: " + e);
                System.exit(1);
            }
        }
    };

    public static Thunk1<SocketChannel> readCallback = 
            new Thunk1<SocketChannel>() {
        public void run(SocketChannel channel) {
            while (true) {
                try {
                    ByteBuffer buf = ByteBuffer.wrap(new byte [1024]);
                    int n = channel.read(buf);
                    if (n > 0)
                        System.out.print(new String(buf.array(), 0, n));
                    else if (n == 0)
                        break;
                    else {
                        channel.close();
                        System.exit(0);
                    }
                }
                catch (IOException e) {
                    System.err.println("Could not read: " + e);
                    System.exit(1);
                }
            }
        }
    };

    public static void main(String [] args) throws IOException {
        if (args.length < 1) {
            System.err.println("usage: java HttpGet <host> <port>");
            System.exit(1);
        }
        acore = new ASyncCoreImpl();
        SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(false);
        channel.connect(new InetSocketAddress(
                    args[0], Integer.parseInt(args[1])));
        acore.registerSelectable(channel, OP_CONNECT, 
                                 curry(acceptCallback, channel));
        acore.asyncMain();
    }
}
