/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import org.acplt.oncrpc.OncRpcProtocols;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import ostore.util.ByteUtils; // for print_bytes function only

/**
 * A simple test.
 *
 * @author Sean C. Rhea
 * @version $Id: GatewayTest.java,v 1.17 2004/11/13 20:34:23 srhea Exp $
 */
public class GatewayTest {

    public static class PutInfo {
        public byte [] key;
        public long expiry_time_ms;
        public PutInfo (byte [] k, long e) { key = k; expiry_time_ms = e; }
        public int hashCode () {
            int result = 0;
            for (int i = 0; i < key.length; ++i)
                result = result * 7 + (((int) key [i]) & 0xff);
            return result;
        }
        public boolean equals (Object rhs) {
            PutInfo other = (PutInfo) rhs;
            if (other.key.length != key.length)
                return false;
            for (int i = 0; i < key.length; ++i)
                if (key [i] != other.key [i])
                    return false;
            return true;
        }
    }

    public static class ClientThread extends Thread {

        protected Random rand;
        protected Set puts = new HashSet ();
        protected InetAddress gateway_host;
        protected int gateway_port;
        protected Logger logger;
        protected MessageDigest digest;
        protected gateway_protClient client;

        public ClientThread (InetAddress h, int p, int seed) {
            rand = new Random (seed); gateway_host = h; gateway_port = p;
            logger = Logger.getLogger (ClientThread.class);
            try { digest = MessageDigest.getInstance("SHA"); }
            catch (Exception e) { assert false; }
        }

        public void run () {

            // Connect to the gateway.

            try {
                sleep (rand.nextInt (5000));
            }
            catch (InterruptedException e) {}

            logger.info ("Connecting to host.");

            try {
                client = new gateway_protClient (gateway_host,
                        gateway_port, OncRpcProtocols.ONCRPC_TCP);
            }
            catch (Exception e) {
                logger.fatal ("Couldn't connect", e);
                System.exit (1);
            }

            while (true) {
                double which = rand.nextDouble ();

                // Make puts fall off as there are more and more values stored.
                if (which <= (1.0 / (puts.size ()/20 + 1))) {
                    // Do a put.

                    bamboo_put_args put = new bamboo_put_args ();

                    int size = rand.nextInt (1005) + 20;
                    put.application = "bamboo.dht.GatewayTest $Revision: 1.17 $";
                    put.client_library = "oncrpc";
                    put.value = new bamboo_value ();
                    put.value.value = new byte [size];
                    rand.nextBytes (put.value.value);
                    put.key = new bamboo_key ();
                    put.key.value = digest.digest (put.value.value);
                    put.ttl_sec = (rand.nextInt (10) + 1)*60;
                    int put_res = 0;
                    long start_time_ms = System.currentTimeMillis ();
                    try {
                        put_res = client.BAMBOO_DHT_PROC_PUT_2 (put);
                    }
                    catch (Exception e) {
                        logger.fatal ("Couldn't put", e);
                        System.exit (1);
                    }
                    long latency_ms =
                        System.currentTimeMillis () - start_time_ms;
                    if (put_res != bamboo_stat.BAMBOO_OK) {
                        logger.info ("Put failed (code=" + put_res
                                + "): size="
                                + put.value.value.length + " key="
                                + ByteUtils.print_bytes (put.key.value)
                                + " ttl_sec=" + put.ttl_sec + " lat="
                                + latency_ms + " ms");
                    }
                    else {
                        logger.info("Put successful: size="
                                    + put.value.value.length + " key="
                                    + ByteUtils.print_bytes(put.key.value, 0, 4)
                                    + " ttl_sec=" + put.ttl_sec + " lat="
                                    + latency_ms + " ms");

                        PutInfo pi = new PutInfo(put.key.value,
                                                 System.currentTimeMillis() +
                                                 put.ttl_sec * 1000);

                        assert!puts.contains(pi);
                        puts.add(pi);
                    }
                }
                else {
                    // Do a get.

                    // Choose a key.

                    PutInfo pi = null;
                    while (true) {

                        if (puts.size () == 0)
                            break;

                        int which_one = rand.nextInt (puts.size ());
                        Iterator i = puts.iterator ();
                        while (i.hasNext ()) {
                            pi = (PutInfo) i.next ();

                            // Don't look for things that are about to expire.
                            if (pi.expiry_time_ms <
                                    System.currentTimeMillis () + 30*1000) {
                                i.remove ();
                                pi = null;
                                continue;
                            }
                            else if (which_one-- == 0) {
                                break;
                            }
                        }

                        if (pi != null)
                            break;
                    }

                    if (pi != null) {

                        bamboo_get_args get_args = new bamboo_get_args ();
                        get_args.application = 
                            "bamboo.dht.GatewayTest $Revision: 1.17 $";
                        get_args.client_library = "oncrpc";
                        get_args.key = new bamboo_key ();
                        get_args.key.value = pi.key;
                        get_args.maxvals = 1;
                        get_args.placemark = new bamboo_placemark ();
                        get_args.placemark.value = new byte [] {};

                        long now_ms = System.currentTimeMillis ();

                        // Try for 20 seconds to retrieve it.

                        while (true) {

                            bamboo_get_res get_res = null;
                            try {
                                get_res =
                                    client.BAMBOO_DHT_PROC_GET_2 (get_args);
                            }
                            catch (Exception e) {
                                logger.fatal ("Couldn't get", e);
                                System.exit (1);
                            }
                            long latency_ms =
                                System.currentTimeMillis () - now_ms;

                            if (get_res.values.length == 0) {
                                logger.info ("Get failed: key=" +
                                        ByteUtils.print_bytes (pi.key, 0, 4));
                                if (System.currentTimeMillis ()
                                        < now_ms + 20*1000) {
                                    logger.info ("trying again");
                                }
                                else {
                                    logger.info ("giving up, lat="
                                            + latency_ms + " ms");
                                    break;
                                }
                            }
                            else {

                                PutInfo pi2 = new PutInfo (digest.digest (
                                            get_res.values [0].value), 0);
                                if (! pi.equals (pi2)) {
                                    logger.info ("Get got bad data: key="
                                            + ByteUtils.print_bytes (pi.key)
                                            + " value hash="
                                            + ByteUtils.print_bytes (pi2.key)
                                            + " lat="
                                            + latency_ms + " ms");
                                }
                                else {
                                    logger.info ("Get successful: key="
                                            + ByteUtils.print_bytes (
                                                pi.key, 0, 4) + " lat="
                                            + latency_ms + " ms");
                                }

                                break;
                            }


                            try {
                                sleep (1000);
                            }
                            catch (InterruptedException e) {}
                        }
                    }
                }

                long sleep_time = rand.nextInt (9001) + 1000;
                try {
                    sleep (sleep_time);
                }
                catch (InterruptedException e) {}
            }
        }
    }


    public static void main (String [] args) throws Exception {

        PatternLayout pl = new PatternLayout ("%d{ISO8601} %-5p %c: %m\n");
        ConsoleAppender ca = new ConsoleAppender (pl);
        Logger.getRoot ().addAppender (ca);
        Logger.getRoot ().setLevel (Level.INFO);

        if (args.length != 4) {
            Logger.getLogger (GatewayTest.class).fatal (
                    "usage: <gateway host> <gateway port> <clients> <seed>");
            System.exit (1);
        }

        InetAddress host = InetAddress.getByName (args [0]);
        int port = Integer.parseInt (args [1]);
        int clients = Integer.parseInt (args [2]);
        int seed = Integer.parseInt (args [3]);

        for (int i = 0; i < clients; ++i) {
            ClientThread t = new ClientThread (host, port, ++seed);
            t.start ();
        }
    }
}

