/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.util;
import bamboo.lss.ASyncCore;
import bamboo.sim.EventQueue;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import ostore.util.ByteArrayOutputBuffer;
import ostore.util.OutputBuffer;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import static bamboo.util.Curry.*;
import static java.nio.channels.SelectionKey.*;

/**
 * Responds to OASIS keep-alive requests.
 *
 * @author Sean C. Rhea
 * @version $Id: Oasis.java,v 1.1 2006/01/24 04:25:26 srhea Exp $
 */
public class Oasis extends bamboo.util.StandardStage {

    protected ServerSocketChannel ssc;
    protected int secret;

    public void init(ConfigDataIF config) throws Exception {
        super.init(config);
        secret = config.getInt("secret");
        ssc = ServerSocketChannel.open();
        ssc.socket().bind(new InetSocketAddress(config.getInt("port")));
        ssc.configureBlocking(false);
        acore.registerSelectable(ssc, OP_ACCEPT, accept);
    }

    protected Runnable accept = new Runnable() {
        public void run() {
            try {
                SocketChannel sc = ssc.accept();
                if (sc != null) {
                    sc.configureBlocking(false);
                    byte bytes[] = new byte[12];
                    OutputBuffer ob = new ByteArrayOutputBuffer(bytes);
                    ob.add(secret); ob.add(1); ob.add(10);
                    acore.registerSelectable(sc, OP_WRITE,  
                            curry(write, sc, ByteBuffer.wrap(bytes))); 
                }
            }
            catch (IOException e) { BUG(e); }
        }
    };

    protected Thunk2<SocketChannel,ByteBuffer> write = 
        new Thunk2<SocketChannel,ByteBuffer>() {

        public void done(SocketChannel sc) {
            acore.unregisterSelectable(sc);
            try { sc.close(); } catch (IOException e) {}
        }
        public void run(SocketChannel sc, ByteBuffer resp) {
            while (true) {
                try {
                    if (sc.write(resp) == 0) break;
                    if (resp.position() == resp.limit()) { done(sc); return; }
                }
                catch (IOException e) { done(sc); return; }
            }
        }
    };
}

