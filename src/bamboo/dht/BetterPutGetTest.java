/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht;
import bamboo.util.Pair;
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
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.LinkedList;
import seda.sandStorm.api.ConfigDataIF;
import static bamboo.util.Curry.*;
import static bamboo.util.GuidTools.guid_to_string;
import static bamboo.util.StringUtil.*;
import static bamboo.openhash.redir.RedirClient.bi2bytes;
import static bamboo.openhash.redir.RedirClient.bytes2bi;
import static ostore.util.ByteUtils.print_bytes;

/**
 * A put and get test for PlanetLab.
 * Also, we've completely abandoned the SandStorm model here except for
 * startup (in the init function); everything is functions and callbacks.
 *
 * @author Sean C. Rhea
 * @version $Id: BetterPutGetTest.java,v 1.4 2005/07/08 22:01:21 srhea Exp $
 */
public class BetterPutGetTest extends StandardStage {

    public static double random_exponential(double mean, Random rand) {
        double u = rand.nextDouble();
        return (0 - (mean * Math.log(1.0 - u)));
    }

    protected static boolean arrayeq(byte[] a, int aOffset, int aLen, 
                                     byte[] b, int bOffset, int bLen) {
        if (aLen != bLen) 
            return false;
        int done = aOffset + aLen;
        while (aOffset < done) {
            if (a[aOffset++] != b[bOffset++])
                return false;
        }
        return true;
    }

    // These timeouts are for how long until the Gateway should send us
    // _some_ response.

    public long GET_TIMEOUT_MS = 60*60*1000;
    public long PUT_TIMEOUT_MS = 60*60*1000;

    // How long before a put expires will we stop looking for it.

    public long GET_SLOP_MS = 60*60*1000;

    // This timeout is how long we'll keep trying to get a value, even though
    // the result of the get keeps coming back saying that there are no
    // values.

    public long MAX_GET_TIME_MS = 60*60*1000;

    public boolean die_on_failure;

    public PrintWriter log;

    public static class PutInfo implements Comparable {
        public byte[] key;
        public int index;
        public long expiry_time_ms;
        public PutInfo(byte [] k, int i, long e) {
            key = k; index = i; expiry_time_ms = e;
        }
        public int hashCode() {
            ByteBuffer bb = ByteBuffer.wrap(key);
            return bb.getInt();
        }
        public boolean equals(Object rhs) {
            PutInfo other =(PutInfo) rhs;
            if (index != other.index)
                return false;
            if (other.key.length != key.length)
                return false;
            for (int i = 0; i < key.length; ++i)
                if (key [i] != other.key [i])
                    return false;
            return true;
        }
        public int compareTo(Object rhs) {
            PutInfo other =(PutInfo) rhs;
            for (int i = 0; i < key.length; ++i) {
                if (key [i] < other.key [i])
                    return -1;
                else if (key [i] > other.key [i])
                    return 1;
            }
            if (index < other.index)
                return -1;
            else if (index > other.index)
                return 1;
            return 0;
        }
        public String toString() {
            return "(0x" + guid_to_string(bytes2bi(key)) + ", " + index + ")";
        }
    }

    public static class GetInfo {
        public bamboo_get_args get_args;
        public BigInteger key;
        public Set<PutInfo> pis;
        public long start_time_ms;
        public int attempt = 1;
        public GetInfo(bamboo_get_args g, BigInteger k, Set<PutInfo> p, long s){
            get_args = g; key = k; pis = p; start_time_ms = s;
        }
        public String getTypeName() {
            return getTypeNames.get(new Integer(get_args.maxvals));
        }
    }

    protected Random rand;
    protected Map<BigInteger,Set<PutInfo>> puts = 
        new HashMap<BigInteger,Set<PutInfo>>();
    protected MessageDigest digest;
    protected GatewayClient client;
    protected double mean_get_period_ms, mean_put_period_ms;
    protected int storage_goal;
    protected int totalStorage, totalValues;
    protected int putTTL = 7*24*60*60;
    protected int putSize = 32;
    protected int desiredPutSetSize = 3, maxPutSetSize = 5;
    protected long next_seq;
    protected Map<Long,GetInfo> pending_gets = new HashMap<Long,GetInfo>();

    protected void scheduleNextGet() {
        long s = Math.round(random_exponential(mean_get_period_ms, rand));
        logger.info("Next get in " + s + " ms");
        acore.registerTimer(s, nextGet);
    }

    protected void scheduleNextPut() {
        long s = Math.round(random_exponential(mean_put_period_ms, rand));
        logger.info("Next put in " + s + " ms");
        acore.registerTimer(s, nextPut);
    }

    protected int randomIndex() {
        return rand.nextInt(Integer.MAX_VALUE);
    }

    public int getIndex(byte[] data) {
        return ByteBuffer.wrap(data).getInt();
    }

    protected byte[] makeData(byte[] key, int index) {
        ByteBuffer seed = ByteBuffer.allocate(key.length + 4);
        seed.put(key);
        seed.putInt(index);
        SecureRandom srand = null;
        try {
            srand = SecureRandom.getInstance("SHA1PRNG");
        }
        catch (Exception e) {
            assert false : e;
        }
        srand.setSeed(seed.array());
        byte[] tail = new byte[putSize - 4];
        srand.nextBytes(tail);
        byte[] result = new byte[putSize]; 
        ByteBuffer bb = ByteBuffer.wrap(result);
        bb.putInt(index);
        bb.put(tail);
        // logger.info("makeData(0x" + bytes_to_str(key) + ", " + index
        //             + ")=0x" + bytes_to_str(result));
        return result;
    }

    protected bamboo_put_args makePut(byte[] key, int index) {
        bamboo_put_args put = new bamboo_put_args();
        put.application = "bamboo.dht.BetterPutGetTest $Revision: 1.4 $";
        // GatewayClient will fill in put.client_library
        put.key = new bamboo_key();
        put.key.value = key;
        put.value = new bamboo_value();
        put.value.value = makeData(key, index);
        put.ttl_sec = putTTL;
        return put;
    }

    public boolean verifyData(byte[] key, int index, byte[] found) {
        byte[] expected = makeData(key, index);
        return java.util.Arrays.equals(expected, found);
    }

    public Runnable nextPut = new Runnable() {
	public void run() {
            if (totalStorage < storage_goal) {

                Integer index = null;
                BigInteger key = null;
                byte[] keyBytes = null;
                if (!puts.isEmpty() 
                    && (totalValues / puts.size() < desiredPutSetSize)) {
                    // Do another put with an existing key.
                    Set<PutInfo> pis = null;
                    for (int tries = 0; tries < 10; ++tries) {
                        int which = rand.nextInt(puts.size());
                        Iterator<BigInteger> i = puts.keySet().iterator();
                        while (which-- > 0) i.next();
                        key = i.next();
                        keyBytes = bi2bytes(key);
                        pis = puts.get(key);
                        int idx = randomIndex();
                        if ((pis.size() < maxPutSetSize) &&
                            !pis.contains(new PutInfo(keyBytes, idx, 0))) {
                            index = new Integer(idx);
                            break;
                        }
                        else {
                            key = null;
                            keyBytes = null;
                        }
                    }
                }
                if (key == null) {
                    // Do a new put.
                    keyBytes = new byte[20];
                    rand.nextBytes(keyBytes);
                    key = bytes2bi(keyBytes);
                    index = new Integer(randomIndex());
                }

                bamboo_put_args putArgs = makePut(keyBytes, index.intValue());
                logger.info("Doing a put with key=0x"
                            + bytes_to_str(putArgs.key.value) 
                            + " index=" + index 
                            + " data=0x" + bytes_to_str(putArgs.value.value));
                long start_time_ms = timer_ms();
                Object token = acore.registerTimer(PUT_TIMEOUT_MS, 
                        curry(putTimeout, key, index));
                client.put(putArgs, curry(putDone, putArgs, key, index, 
                           new Long(start_time_ms), token));
            }

            scheduleNextPut();
	}
    };

    protected Thunk2<BigInteger,Integer> putTimeout = 
        new Thunk2<BigInteger,Integer>() {
        public void run(BigInteger key, Integer index) {
            logger.warn("Put timed out: key=0x" + guid_to_string(key)
                        + " index=" + index);
            if (die_on_failure)
                System.exit(1);
        }
    };

    protected Thunk6<bamboo_put_args,BigInteger,Integer,Long,Object,Integer> 
        putDone = 
        new Thunk6<bamboo_put_args,BigInteger,Integer,Long,Object,Integer>() {

        public void run(bamboo_put_args put, BigInteger key, Integer index,
                        Long start_time_ms, Object token, Integer put_res) {

            acore.cancelTimer(token);

            long latency_ms = timer_ms() - start_time_ms.longValue();
            if (put_res.intValue() != bamboo_stat.BAMBOO_OK) {
                logger.info("Put failed: key=" + guid_to_string(key)
                            + " index=" + index + " lat=" + latency_ms 
                            + " ms, reason=" + put_res);
            }
            else {
                logger.info("Put successful: key=0x" + guid_to_string(key)
                            + " index=" + index + " lat=" + latency_ms + " ms");

                Set<PutInfo> pis = puts.get(key);
                if (pis == null) {
                    pis = new TreeSet<PutInfo>();
                    puts.put(key, pis);
                }

                long expire_delay_ms = put.ttl_sec * 1000 - latency_ms;
                PutInfo pi = new PutInfo(put.key.value, index.intValue(),
                        now_ms() + expire_delay_ms);

                if (pis.add(pi)) {
                    // There's a slight chance we picked the same index twice.
                    totalStorage += putSize;
                    totalValues += 1;
                }
                if (log != null)
                    logPut(pi, log);

                acore.registerTimer(expire_delay_ms - GET_SLOP_MS, 
                                    curry(putExpired, key, pi));
            }
        }
    };

    protected Thunk2<BigInteger,PutInfo> putExpired = 
        new Thunk2<BigInteger,PutInfo>() {
        public void run(BigInteger key, PutInfo pi) {
            logger.info("put expired: key=0x" + guid_to_string(key) 
                        + " index=" + pi.index);
            Set<PutInfo> pis = puts.get(key);
            pis.remove(pi);
            // TODO: If we accidentally added two with the same index, this
            // could be wrong...
            totalStorage -= putSize;
            totalValues -= 1;
            if (pis.isEmpty())
                puts.remove(key);
        }
    };

    public static int getTypes[] = new int[] {
        Dht.MAXVALS_MASK | Dht.ROOT_ONLY   | Dht.PNS_ONLY,
        Dht.MAXVALS_MASK | Dht.ROOT_ONLY   | Dht.PRS,
        Dht.MAXVALS_MASK | Dht.ROOT_ONLY   | Dht.SCALED_PRS,
        Dht.MAXVALS_MASK | Dht.ROOT_ONLY   | Dht.SCALED_PRS_FD,
        Dht.MAXVALS_MASK | Dht.FIRST_ONLY  | Dht.PNS_ONLY,
        Dht.MAXVALS_MASK | Dht.FIRST_ONLY  | Dht.PRS,
        Dht.MAXVALS_MASK | Dht.FIRST_ONLY  | Dht.SCALED_PRS,
        Dht.MAXVALS_MASK | Dht.FIRST_ONLY  | Dht.SCALED_PRS_FD,
        Dht.MAXVALS_MASK | Dht.QUORUM      | Dht.PNS_ONLY,
        Dht.MAXVALS_MASK | Dht.QUORUM      | Dht.PRS,
        Dht.MAXVALS_MASK | Dht.QUORUM      | Dht.SCALED_PRS,
        Dht.MAXVALS_MASK | Dht.QUORUM      | Dht.SCALED_PRS_FD,
        Dht.MAXVALS_MASK | Dht.QUORUM_SYNC | Dht.PNS_ONLY,
        Dht.MAXVALS_MASK | Dht.QUORUM_SYNC | Dht.PRS,
        Dht.MAXVALS_MASK | Dht.QUORUM_SYNC | Dht.SCALED_PRS,
        Dht.MAXVALS_MASK | Dht.QUORUM_SYNC | Dht.SCALED_PRS_FD
    };

    public static Map<Integer,String> getTypeNames;

    static {
        getTypeNames = new HashMap<Integer,String>();
        getTypeNames.put(new Integer(getTypes[0]), "ROOT_ONLY, PNS_ONLY");
        getTypeNames.put(new Integer(getTypes[1]), "ROOT_ONLY, PRS");
        getTypeNames.put(new Integer(getTypes[2]), "ROOT_ONLY, SCALED_PRS");
        getTypeNames.put(new Integer(getTypes[3]), "ROOT_ONLY, SCALED_PRS_FD");
        getTypeNames.put(new Integer(getTypes[4]), "FIRST_ONLY, PNS_ONLY");
        getTypeNames.put(new Integer(getTypes[5]), "FIRST_ONLY, PRS");
        getTypeNames.put(new Integer(getTypes[6]), "FIRST_ONLY, SCALED_PRS");
        getTypeNames.put(new Integer(getTypes[7]), "FIRST_ONLY, SCALED_PRS_FD");
        getTypeNames.put(new Integer(getTypes[8]), "QUORUM, PNS_ONLY");
        getTypeNames.put(new Integer(getTypes[9]), "QUORUM, PRS");
        getTypeNames.put(new Integer(getTypes[10]), "QUORUM, SCALED_PRS");
        getTypeNames.put(new Integer(getTypes[11]), "QUORUM, SCALED_PRS_FD");
        getTypeNames.put(new Integer(getTypes[12]), "QUORUM_SYNC, PNS_ONLY");
        getTypeNames.put(new Integer(getTypes[13]), "QUORUM_SYNC, PRS");
        getTypeNames.put(new Integer(getTypes[14]), "QUORUM_SYNC, SCALED_PRS");
        getTypeNames.put(new Integer(getTypes[15]), "QUORUM_SYNC, SCALED_PRS_FD");
    };

    protected LinkedList<Integer> makeShuffle() {
        LinkedList<Integer> shuffled = new LinkedList<Integer>();
        LinkedList<Integer> unshuffled = new LinkedList<Integer>();
        for (int i = 0; i < getTypes.length; ++i)
            unshuffled.addLast(new Integer(getTypes[i]));
        while (!unshuffled.isEmpty()) {
            int which = rand.nextInt(unshuffled.size());
            Iterator<Integer> i = unshuffled.iterator();
            while (true) {
                Integer maxvals = i.next();
                if (which-- == 0) {
                    shuffled.addLast(maxvals);
                    i.remove();
                    break;
                }
            }
        }
        return shuffled;
    }

    protected LinkedList<Integer> shuffle = new LinkedList<Integer>();
    protected boolean bogus;

    protected int nextType() {
        if (shuffle.isEmpty()) {
            shuffle = makeShuffle();
            bogus = rand.nextBoolean();
        }
        return shuffle.removeFirst().intValue();
    }

    public Runnable nextGet = new Runnable() {
        public void run() {

            // Do a get.

            int maxvals = nextType();
            BigInteger key = null;
            Set<PutInfo> pis = null;

            // Half of the time, look for things that should be there.
            if (!puts.isEmpty() && !bogus) {

                // We remove items from puts an hour before they expire, so we
                // can take any put in there and be confident we'll find it in
                // time if it's in the DHT.

                int which = rand.nextInt(puts.size());
                Iterator<BigInteger> i = puts.keySet().iterator();
                while (which-- > 0) i.next();
                key = i.next();

                // We need to copy this set, so that it doesn't change during
                // the get.

                pis = new TreeSet<PutInfo>();
                pis.addAll(puts.get(key));
            }
            if (pis == null) {
                byte[] kb = new byte [20];
                rand.nextBytes(kb);
                key = bytes2bi(kb);
            }

            /*
            LinkedList<Integer> shuffle = makeShuffle();
            for (Integer maxvals : shuffle) {
            */
                bamboo_get_args get_args = new bamboo_get_args();
                get_args.application = 
                    "bamboo.dht.BetterPutGetTest $Revision: 1.4 $";
                // GatewayClient will fill in get_args.client_library
                get_args.key = new bamboo_key();
                get_args.key.value = bi2bytes(key);
                get_args.placemark = new bamboo_placemark();
                get_args.placemark.value = new byte[] {};
                get_args.maxvals = maxvals;

                logger.info("Doing a " + ((pis == null) ? "bogus" : "real")
                        + " " + getTypeNames.get(new Integer(maxvals))
                        + " get: key=0x" + guid_to_string(key));

                long start_time_ms = timer_ms();
                GetInfo gi = new GetInfo(get_args, key, pis, start_time_ms);
                Object token = 
                    acore.registerTimer(GET_TIMEOUT_MS, curry(getTimeout, gi));
                client.get(get_args, curry(getDone, gi, token));
            /*
            }
            */

            scheduleNextGet();
        }
    };

    public Thunk1<GetInfo> getTimeout = new Thunk1<GetInfo>() {
        public void run(GetInfo gi) {
            logger.warn(((gi.pis == null) ? "Bogus" : "Real") + " " 
                        + gi.getTypeName()
                        + " get timed out: key=0x"
                        + guid_to_string(gi.key));
            if (die_on_failure) 
                System.exit (1);
        }
    };

    public Thunk3<GetInfo,Object,bamboo_get_res> getDone = 
        new Thunk3<GetInfo,Object,bamboo_get_res>(){
        public void run(GetInfo gi, Object token, bamboo_get_res get_res) {

            acore.cancelTimer(token);

            long latency_ms = timer_ms() - gi.start_time_ms;

            if (gi.pis == null) {
                if (get_res.values.length == 0) {
                    logger.info("Bogus " 
                                + gi.getTypeName()
                                + " get successful: key=0x" 
                                + guid_to_string(gi.key) + " lat=" 
                                + latency_ms + " ms");
                }
                else {
                    logger.warn("Bogus " 
                                + gi.getTypeName()
                                + " get returned values?!?!: key=0x" 
                                + guid_to_string(gi.key) + " lat=" 
                                + latency_ms + " ms");
                }
            }
            else {
                Set<PutInfo> missing = new TreeSet<PutInfo>();
                Set<PutInfo> extra = new TreeSet<PutInfo>();
                Set<PutInfo> bad = new TreeSet<PutInfo>();
                missing.addAll(gi.pis);
                for (int i = 0; i < get_res.values.length; ++i) {
                    byte[] found = get_res.values[i].value;
                    int index = getIndex(found);
                    PutInfo tmp = new PutInfo(gi.get_args.key.value, index, 0);
                    if (!missing.remove(tmp))
                        extra.add(tmp);
                    else if (!verifyData(gi.get_args.key.value, index, found)){
                        bad.add(tmp);
                        // byte[] expected = 
                        //     makeData(gi.get_args.key.value, index);
                        // logger.info("key=0x" 
                        //         + bytes_to_str(gi.get_args.key.value) 
                        //         + " index=" + index);
                        // logger.info("expected: 0x" + bytes_to_str(expected));
                        // logger.info("found:    0x" + bytes_to_str(found));
                    }
                }
                if (missing.isEmpty() && bad.isEmpty()) {
                    String estr = "";
                    for (PutInfo pi : extra) estr += pi.index + " ";
                    estr = estr.trim();
                    logger.info("Real " 
                                + gi.getTypeName()
                                + " get successful: key=0x"
                                + guid_to_string(gi.key) + " count=" 
                                + gi.pis.size() + " lat=" + latency_ms 
                                + " ms" + (extra.isEmpty() ? "" : 
                                           (", extras=[" + estr + "]")));
                }
                else {
                    String mstr = "";
                    for (PutInfo pi : missing) mstr += pi.index + " ";
                    mstr = mstr.trim();
                    String bstr = "";
                    for (PutInfo pi : bad) bstr += pi.index + " ";
                    bstr = bstr.trim();

                    int retry_time = gi.attempt * 10 * 1000;
                    retry_time = retry_time / 2 + rand.nextInt(retry_time);
                    if (timer_ms() + retry_time
                        < gi.start_time_ms + MAX_GET_TIME_MS) {

                        logger.info("Real " 
                                    + gi.getTypeName()
                                    + " get wrong: key=0x" 
                                    + guid_to_string(gi.key) 
                                    + (missing.isEmpty() ? "" : 
                                       (" missing=[" + mstr + "]"))
                                    + (bad.isEmpty() ? "" : 
                                       (" bad=[" + bstr + "]"))
                                    + ".  Trying again.");
                        acore.registerTimer(rand.nextInt(retry_time),
                                            curry(getAgain, gi));
                        ++gi.attempt;
                    }
                    else {
                        logger.warn("Real " 
                                    + gi.getTypeName()
                                    + " get failed: key=0x" 
                                    + guid_to_string(gi.key) 
                                    + (missing.isEmpty() ? "" : 
                                       (" missing=[" + mstr + "]"))
                                    + (bad.isEmpty() ? "" : 
                                       (" bad=[" + bstr + "]"))
                                    + ".  Trying again.");
                        if (die_on_failure)
                            System.exit (1);
                    }
                }
            }
        }
    };

    public Thunk1<GetInfo> getAgain = new Thunk1<GetInfo>() {
        public void run(GetInfo gi) {
            Object token = 
                acore.registerTimer(GET_TIMEOUT_MS, curry(getTimeout, gi));
            client.get(gi.get_args, curry(getDone, gi, token));
        }
    };

    protected void logPut(PutInfo pi, PrintWriter out) {
        out.print(bytes2bi(pi.key).toString(16));
        out.print(" ");
        out.print(pi.index);
        out.print(" ");
        out.println(pi.expiry_time_ms);
        out.flush();
    }

    protected void recover(String put_log_path) {
        File old_log_file = new File(put_log_path);
        if (old_log_file.exists()) {
            File new_log_file = new File(put_log_path + ".recover");
            if (new_log_file.exists()) {
                if (!new_log_file.delete()) {
                    logger.fatal("could not delete " + new_log_file);
                    System.exit(1);
                }
            }
            PrintWriter new_log = null;
            try {
                new_log = new PrintWriter(new BufferedWriter(
                            new FileWriter(new_log_file)));
            }
            catch (IOException e) {
                logger.fatal("couldn't open " + new_log_file + " for writing");
                logger.fatal(e);
                System.exit(1);
            }
            LineNumberReader old_log = null;
            try {
                old_log = new LineNumberReader(
                        new BufferedReader(new FileReader(old_log_file)));
            }
            catch (FileNotFoundException e) {
                logger.fatal("couldn't open " + old_log_file + " for reading");
                logger.fatal(e);
                System.exit(1);
            }
            int lineno = 1;
            while (true) {
                String line = null;
                try { line = old_log.readLine(); }
                catch (IOException e) {
                    logger.fatal("can't read on line " + lineno);
                    logger.fatal(e);
                    System.exit(1);
                }
                if (line == null)
                    break;
                lineno++;
                String rem = line;
                int space = rem.indexOf(" ");
                BigInteger key = new BigInteger(rem.substring(0, space), 16);
                rem = rem.substring(space + 1, rem.length());
                space = rem.indexOf(" ");
                int index = Integer.parseInt(rem.substring(0, space));
                rem = rem.substring(space + 1, rem.length());
                long expiry_time_ms = Long.parseLong(rem);
                if (expiry_time_ms - GET_SLOP_MS > now_ms()) {
                    PutInfo pi = 
                        new PutInfo(bi2bytes(key), index, expiry_time_ms);
                    totalStorage += putSize;
                    totalValues += 1;
                    Set<PutInfo> pis = puts.get(key);
                    if (pis == null) {
                        pis = new TreeSet<PutInfo>();
                        puts.put(key, pis);
                    }
                    pis.add(pi);
                    logPut(pi, new_log);
                    acore.registerTimer(expiry_time_ms - now_ms() -GET_SLOP_MS, 
                                        curry(putExpired, key, pi));
                }
            }
            new_log.close();
            try { old_log.close(); }
            catch (IOException e) { BUG(e); }
            if (!new_log_file.renameTo(old_log_file)) {
                logger.fatal("couldn't rename " + new_log_file + " to " 
                        + old_log_file);
                System.exit(1);
            }
        }
        try {
            log = new PrintWriter(new BufferedWriter(
                        new FileWriter(old_log_file, true))); // append
        }
        catch (IOException e) {
            logger.fatal("couldn't open " + old_log_file + " for reading");
            logger.fatal(e);
            System.exit(1);
        }
    }

    public void init(ConfigDataIF config) throws Exception {
	super.init(config);
        String put_log_path = config_get_string(config, "put_log_path");
        if (put_log_path != null)
            recover(put_log_path);
        die_on_failure = config_get_boolean(config, "die_on_failure");
        mean_put_period_ms = config_get_int(config, "mean_put_period_ms");
        if (mean_put_period_ms == -1.0)
            mean_put_period_ms = 60.0*1000.0;
        mean_get_period_ms = config_get_int(config, "mean_get_period_ms");
        if (mean_get_period_ms == -1.0)
            mean_get_period_ms = 60.0*1000.0;
        mean_get_period_ms /= getTypes.length;
        storage_goal = config_get_int(config, "storage_goal");
        if (storage_goal == -1)
            storage_goal = 1024*1024*1024;
        int seed = config_get_int(config, "seed");
        if (seed == -1)
            seed = ((int) now_ms()) ^ my_node_id.hashCode();
        rand = new Random(seed);
        try { digest = MessageDigest.getInstance("SHA"); }
        catch (Exception e) { assert false; }
        String client_stg_name =
            config_get_string(config, "client_stage_name");
        client = (GatewayClient) lookup_stage(config, client_stg_name);
        scheduleNextPut();
        scheduleNextGet();
    }
}

