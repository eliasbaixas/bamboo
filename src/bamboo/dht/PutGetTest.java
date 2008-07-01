/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht;
import bamboo.util.StandardStage;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import seda.sandStorm.api.ConfigDataIF;
import static bamboo.util.Curry.*;
import static bamboo.openhash.redir.RedirClient.bi2bytes;
import static bamboo.openhash.redir.RedirClient.bytes2bi;
import static ostore.util.ByteUtils.print_bytes;

/**
 * A put and get test for PlanetLab.
 * Also, we've completely abandoned the SandStorm model here except for
 * startup (in the init function); everything is functions and callbacks.
 *
 * @author Sean C. Rhea
 * @version $Id: PutGetTest.java,v 1.17 2005/03/02 02:25:13 srhea Exp $
 */
public class PutGetTest extends StandardStage {

    // These timeouts are for how long until the Gateway should send us
    // _some_ response.

    public long GET_TIMEOUT_MS = 60*60*1000;
    public long PUT_TIMEOUT_MS = 60*60*1000;

    // This timeout is how long we'll keep trying to get a value, even though
    // the result of the get keeps coming back saying that there are no
    // values.

    public long MAX_GET_TIME_MS = 60*60*1000;

    public boolean die_on_failure;

    public PrintWriter log;

    public static class PutInfo {
        public byte [] key;
        public long expiry_time_ms;
        public int size;
        public PutInfo (byte [] k, long e, int s) {
            key = k; expiry_time_ms = e; size = s;
        }
        public int hashCode () {
            ByteBuffer bb = ByteBuffer.wrap (key);
            return bb.getInt ();
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

    public static class GetInfo {
        public bamboo_get_args get_args;
        public PutInfo pi;
        public long start_time_ms;
        public int attempt = 1;
        public Long seq;
        public GetInfo (bamboo_get_args g, PutInfo p, long s, Long q) {
            get_args = g; pi = p; start_time_ms = s; seq = q;
        }
    }

    protected Random rand;
    protected HashSet<PutInfo> puts = new HashSet<PutInfo> ();
    protected MessageDigest digest;
    protected GatewayClient client;
    protected double mean_get_period_ms, mean_put_period_ms;
    protected int storage_goal;
    protected int total_storage;

    public static double random_exponential(double mean, Random rand) {
        double u = rand.nextDouble();
        return (0 - (mean * Math.log(1.0 - u)));
    }

    protected void next_get_op () {
        long s = Math.round (random_exponential (mean_get_period_ms, rand));
        logger.info ("Next get in " + s + " ms");
        acore.register_timer (s, next_get_op_cb);
    }

    protected void next_put_op () {
        long s = Math.round (random_exponential (mean_put_period_ms, rand));
        logger.info ("Next put in " + s + " ms");
        acore.register_timer (s, next_put_op_cb);
    }

    protected int [] ttl_values = { 3600, 24*3600, 7*24*3600 };
    protected int [] put_sizes = { 32, 64, 128, 256, 512, 1024 };
    protected long next_seq;
    protected HashMap<Long,bamboo_put_args> pending_puts = 
        new HashMap<Long,bamboo_put_args> ();
    protected HashMap<Long,GetInfo> pending_gets = new HashMap<Long,GetInfo> ();

    public Runnable next_put_op_cb = new Runnable () {
	public void run() {
            if (total_storage < storage_goal) {
                // Do a put.

                bamboo_put_args put = new bamboo_put_args ();
                put.application = "bamboo.dht.PutGetTest $Revision: 1.17 $";
                // GatewayClient will fill in put.client_library
                put.value = new bamboo_value ();
                put.value.value =
                    new byte [put_sizes [rand.nextInt(put_sizes.length)]];
                rand.nextBytes (put.value.value);
                put.key = new bamboo_key ();
                put.key.value = digest.digest (put.value.value);
                put.ttl_sec = ttl_values [rand.nextInt(ttl_values.length)];

                logger.info("Doing a put: size="
                           + put.value.value.length + " key=0x"
                           + print_bytes(put.key.value, 0, 4)
                           + "ttl_sec=" + put.ttl_sec);

                long start_time_ms = timer_ms ();
                Long seq = new Long (next_seq++);
                client.put (put, 
                        curry(put_done_cb, put, new Long(start_time_ms), seq));
                pending_puts.put (seq, put);
                acore.register_timer(PUT_TIMEOUT_MS, curry(put_timeout_cb, seq));
            }

            next_put_op ();
	}
    };

    public Thunk1<Long> put_timeout_cb = new Thunk1<Long> () {
        public void run(Long seq) {
            bamboo_put_args put = (bamboo_put_args) pending_puts.remove (seq);
            if (put != null) {
                logger.warn("Put timed out: size="
                        + put.value.value.length + " key=0x"
                        + print_bytes(put.key.value, 0, 20)
                        + "ttl_sec=" + put.ttl_sec);
                if (die_on_failure)
                    System.exit (1);
            }
        }
    };

    public Runnable next_get_op_cb = new Runnable () {
        public void run() {

            // Do a get.

            // Choose a key: make up to five random choices from our existing
            // puts to try and find one that isn't about to expire.

            PutInfo pi = null;
            for (int tries = 0; tries < 5; ++tries) {

                if (puts.size() == 0)
                    break;

                int which_one = rand.nextInt(puts.size());
                Iterator i = puts.iterator();
                while (i.hasNext()) {
                    pi = (PutInfo) i.next();

                    // Don't look for things that are about to expire.
                    if (pi.expiry_time_ms < timer_ms() + MAX_GET_TIME_MS) {
                        i.remove();
                        total_storage -= pi.size;
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

                 // only have one get outstanding at a time for each value
                puts.remove(pi);

                bamboo_get_args get_args = new bamboo_get_args();
                get_args.application = "bamboo.dht.PutGetTest $Revision: 1.17 $";
                // GatewayClient will fill in get_args.client_library
                get_args.key = new bamboo_key();
                get_args.key.value = pi.key;
                get_args.maxvals = 1;
                get_args.placemark = new bamboo_placemark();
                get_args.placemark.value = new byte[] {};

                logger.info("Doing a get: key=0x" + print_bytes(pi.key, 0, 4));

                long start_time_ms = timer_ms();
                Long seq = new Long (next_seq++);
                GetInfo gi = new GetInfo (get_args, pi, start_time_ms, seq);
                client.get(get_args, curry(get_done_cb, gi));
                pending_gets.put (seq, gi);
                acore.register_timer(GET_TIMEOUT_MS, curry(get_timeout_cb, seq));
            }

            next_get_op();
        }
    };

    public Thunk1<Long> get_timeout_cb = new Thunk1<Long> () {
        public void run(Long seq) {
            GetInfo gi = (GetInfo) pending_gets.remove (seq);
            if (gi != null) {
                logger.fatal ("Get timed out: key=0x"
                        + print_bytes(gi.get_args.key.value, 0, 20));
                if (die_on_failure) 
                    System.exit (1);
                puts.add (gi.pi);
            }
        }
    };

    public Thunk4<bamboo_put_args,Long,Long,Integer> put_done_cb = 
        new Thunk4<bamboo_put_args,Long,Long,Integer> (){

        public void run(bamboo_put_args put, Long start_time_ms, 
                        Long seq, Integer put_res) {

            if (! pending_puts.containsKey (seq)) {
                // Already timed out.
                return;
            }
            pending_puts.remove (seq);
            long latency_ms = timer_ms () - start_time_ms.longValue ();
            if (put_res.intValue () != bamboo_stat.BAMBOO_OK) {
                logger.info ("Put failed: size="
                        + put.value.value.length + " key="
                        + print_bytes (put.key.value, 0, 20)
                        + "ttl_sec=" + put.ttl_sec + " lat="
                        + latency_ms + " ms, reason=" + put_res);
            }
            else {
                logger.info("Put successful: size="
                            + put.value.value.length + " key=0x"
                            + print_bytes(put.key.value, 0, 4)
                            + "ttl_sec=" + put.ttl_sec + " lat="
                            + latency_ms + " ms");

                PutInfo pi = new PutInfo(put.key.value,
                                         timer_ms () - latency_ms
                                         + put.ttl_sec * 1000,
                                         put.value.value.length);

                assert ! puts.contains(pi);
                total_storage += pi.size;
                puts.add(pi);
                if (log != null)
                    log_put (pi, log);
            }
        }
    };

    public Thunk2<GetInfo,bamboo_get_res> get_done_cb = 
        new Thunk2<GetInfo,bamboo_get_res> (){

        public void run(GetInfo gi, bamboo_get_res get_res) {
            long latency_ms = timer_ms () - gi.start_time_ms;
            if (! pending_gets.containsKey (gi.seq)) {
                // Already timed out.
                return;
            }
            pending_gets.remove (gi.seq);

            if (get_res.values.length == 0) {
                int retry_time = gi.attempt*10*1000;
                retry_time = retry_time / 2 + rand.nextInt(retry_time);
                if (timer_ms () + retry_time
                    < gi.start_time_ms + MAX_GET_TIME_MS) {
                    logger.info ("Trying to get key=0x" +
                                 print_bytes (gi.pi.key, 0, 4) + "again");
                    acore.register_timer (rand.nextInt (retry_time),
                                          curry(get_again_cb, gi));
                    ++gi.attempt;
                    return;
                }
                else {
                    logger.info ("Get failed: key=0x" +
                                 print_bytes (gi.pi.key, 0, 20) +
                                 "size=" + gi.pi.size + " ttl remaining=" +
                                 (timer_ms () - gi.pi.expiry_time_ms) + " ms");
                    if (die_on_failure)
                        System.exit (1);
                    puts.add (gi.pi);
                    return;
                }
            }
            else {

                PutInfo pi2 = new PutInfo (digest.digest (
                            get_res.values [0].value), 0, 0);
                if (! gi.pi.equals (pi2)) {
                    logger.info ("Get got bad data: key=0x"
                            + print_bytes (gi.pi.key) + " value hash="
                            + print_bytes (pi2.key) + " lat="
                            + latency_ms + " ms");
                }
                else {
                    logger.info("Get successful: key=0x"
                                + print_bytes(gi.pi.key, 0, 4) + "size="
                                + get_res.values[0].value.length + " lat="
                                + latency_ms + " ms");
                }
                puts.add (gi.pi);
                return;
            }
        }
    };

    public Thunk1<GetInfo> get_again_cb = new Thunk1<GetInfo> () {
        public void run(GetInfo gi) {
            Long seq = new Long (next_seq++);
            gi.seq = seq;
            client.get (gi.get_args, curry(get_done_cb, gi));
            pending_gets.put (seq, gi);
            acore.register_timer (GET_TIMEOUT_MS, curry(get_timeout_cb, seq));
        }
    };

    protected void log_put (PutInfo pi, PrintWriter out) {
        out.print (bytes2bi (pi.key).toString (16));
        out.print (" ");
        out.print (pi.expiry_time_ms);
        out.print (" ");
        out.println (pi.size);
        out.flush ();
    }

    protected void recover (String put_log_path) {
        File old_log_file = new File (put_log_path);
        if (old_log_file.exists ()) {
            File new_log_file = new File (put_log_path + ".recover");
            if (new_log_file.exists ()) {
                if (! new_log_file.delete ()) {
                    logger.fatal ("could not delete " + new_log_file);
                    System.exit (1);
                }
            }
            PrintWriter new_log = null;
            try {
                new_log = new PrintWriter (new BufferedWriter (
                            new FileWriter (new_log_file)));
            }
            catch (IOException e) {
                logger.fatal ("couldn't open " + new_log_file + " for writing");
                logger.fatal (e);
                System.exit (1);
            }
            LineNumberReader old_log = null;
            try {
                old_log = new LineNumberReader (
                        new BufferedReader (new FileReader (old_log_file)));
            }
            catch (FileNotFoundException e) {
                logger.fatal ("couldn't open " + old_log_file + " for reading");
                logger.fatal (e);
                System.exit (1);
            }
            int lineno = 1;
            while (true) {
                String line = null;
                try { line = old_log.readLine (); }
                catch (IOException e) {
                    logger.fatal ("can't read on line " + lineno);
                    logger.fatal (e);
                    System.exit (1);
                }
                if (line == null)
                    break;
                lineno++;
                String rem = line;
                int space = rem.indexOf (" ");
                BigInteger key = new BigInteger (rem.substring (0, space), 16);
                rem = rem.substring (space + 1, rem.length ());
                space = rem.indexOf (" ");
                long expiry_time_ms = Long.parseLong (rem.substring (0, space));
                rem = rem.substring (space + 1, rem.length ());
                int size = Integer.parseInt (rem);
                if (expiry_time_ms > now_ms ()) {
                    PutInfo pi = 
                        new PutInfo (bi2bytes (key), expiry_time_ms, size);
                    total_storage += size;
                    puts.add (pi);
                    log_put (pi, new_log);
                }
            }
            new_log.close ();
            try { old_log.close (); }
            catch (IOException e) { BUG (e); }
            if (!  new_log_file.renameTo (old_log_file)) {
                logger.fatal ("couldn't rename " + new_log_file + " to " 
                        + old_log_file);
                System.exit (1);
            }
        }
        try {
            log = new PrintWriter (new BufferedWriter (
                        new FileWriter (old_log_file, true /* append */)));
        }
        catch (IOException e) {
            logger.fatal ("couldn't open " + old_log_file + " for reading");
            logger.fatal (e);
            System.exit (1);
        }
    }

    public void init (ConfigDataIF config) throws Exception {
	super.init (config);
        String put_log_path = config_get_string (config, "put_log_path");
        if (put_log_path != null)
            recover (put_log_path);
        die_on_failure = config_get_boolean (config, "die_on_failure");
        mean_put_period_ms = config_get_int (config, "mean_put_period_ms");
        if (mean_put_period_ms == -1.0)
            mean_put_period_ms = 60.0*1000.0;
        mean_get_period_ms = config_get_int (config, "mean_get_period_ms");
        if (mean_get_period_ms == -1.0)
            mean_get_period_ms = 60.0*1000.0;
        storage_goal = config_get_int (config, "storage_goal");
        if (storage_goal == -1)
            storage_goal = 1024*1024*1024;
        int seed = config_get_int (config, "seed");
        if (seed == -1)
            seed = ((int) now_ms ()) ^ my_node_id.hashCode();
        rand = new Random (seed);
        try { digest = MessageDigest.getInstance("SHA"); }
        catch (Exception e) { assert false; }
        String client_stg_name =
            config_get_string (config, "client_stage_name");
        client = (GatewayClient) lookup_stage (config, client_stg_name);
        next_put_op ();
        next_get_op ();
    }
}

