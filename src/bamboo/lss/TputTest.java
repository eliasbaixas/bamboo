/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.lss;
import java.nio.ByteBuffer;
import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * A throughput test for UdpCC.
 *
 * @author     Sean C. Rhea
 * @version    $Id: TputTest.java,v 1.10 2005/04/27 20:51:10 srhea Exp $
 */
public class TputTest implements UdpCC.Serializer, UdpCC.Sink, UdpCC.SendCB {

    public int serialize_size (Object msg) {
        ByteBuffer buf = (ByteBuffer) msg;
        return buf.limit ();
    }

    public void serialize (Object msg, ByteBuffer buf) {
        buf.put ((ByteBuffer) msg);
    }

    public Object deserialize (ByteBuffer buf) throws Exception {
        return buf;
    }

    public void recv (Object msg, InetSocketAddress src, 
            InetSocketAddress local,
            int tries, long wait_ms, long est_rtt_ms) {
    }

    public void cb (Object user_data, boolean success) {
        if (! success) {
            System.err.println ("send failed");
            System.exit (1);
        }
        ++cnt;
        send_new_msg ();
    }

    public static ASyncCore.TimerCB stop_cb = new ASyncCore.TimerCB () {
        public void timer_cb (Object not_used) {
            long test_length_ms = System.currentTimeMillis () - start_time_ms;
            System.out.println ("sent " + cnt + " " + msg_size + "-byte " +
                    "packets in " + test_length_ms + " ms.");
            double bw = ((double) (8*cnt*msg_size) / test_length_ms * 1000.0);
            String units = "bits";
            if (bw > 1024.0) { bw /= 1024.0; units = "kbits"; }
            if (bw > 1024.0) { bw /= 1024.0; units = "Mbits"; }
            if (bw > 1024.0) { bw /= 1024.0; units = "Gbits"; }
            System.out.println ("bandwidth = " + bw + " " + units + "/s");
            System.exit (0);
        }
    };

    public static void send_new_msg () {
        udpcc.send (ByteBuffer.allocate (1024), peer, 3600, cbs, null);
    }

    public static UdpCC udpcc;
    public static ASyncCore acore;
    public static InetSocketAddress peer;
    public static TputTest cbs;
    public static int msg_size;
    public static int cnt;
    public static long start_time_ms;

    public static void main (String [] args) throws Exception {
        acore = new ASyncCoreImpl ();
        cbs = new TputTest ();
        int argc = 0;
        int window_size = Integer.parseInt (args [argc++]);
        if (window_size > 0) {
            udpcc = new UdpCC (acore, new InetSocketAddress(0), cbs, cbs);
            int test_length_secs = Integer.parseInt (args [argc++]);
            msg_size = Integer.parseInt (args [argc++]);
            String host = args [argc++];
            int port = Integer.parseInt (args [argc++]);
            peer = new InetSocketAddress (host, port);
            for (int i = 0; i < window_size; ++i) 
                send_new_msg ();
            acore.register_timer (test_length_secs * 1000, stop_cb, null);
            start_time_ms = System.currentTimeMillis ();
        }
        else {
            int port = Integer.parseInt (args [argc++]);
            InetSocketAddress me = new InetSocketAddress (port);
            udpcc = new UdpCC (acore, me, cbs, cbs);
            udpcc.track_duplicates (30*1000);
        }
        acore.async_main ();
    }
}

