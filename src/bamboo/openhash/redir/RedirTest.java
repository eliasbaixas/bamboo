/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.openhash.redir;
import bamboo.lss.ASyncCore;
import bamboo.util.GuidTools;
import bamboo.util.StandardStage;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.util.Random;
import java.util.TreeMap;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;

/**
 * An implementation of ReDiR.
 *
 * @author Sean C. Rhea
 * @version $Id: RedirTest.java,v 1.6 2004/11/19 22:59:27 srhea Exp $
 */
public class RedirTest extends StandardStage
implements SingleThreadedEventHandlerIF {

    protected static final String APP = 
        "bamboo.openhash.redir.RedirTest $Revision: 1.6 $";

    protected InetSocketAddress my_addr;
    protected BigInteger my_id;
    protected RedirClient client;
    protected Random rand;
    protected BigInteger namespace;
    protected MessageDigest digest;
    protected int levels;
    protected double mean_period_ms;
    protected long lookup_delay_ms;

    protected static TreeMap instances = new TreeMap ();

    public void init (ConfigDataIF config) throws Exception {
	super.init (config);
        mean_period_ms = config_get_int (config, "mean_period_ms");
        if (mean_period_ms == -1.0)
            mean_period_ms = 60.0*1000.0;
        levels = config_get_int (config, "levels");
        int seed = config_get_int (config, "seed");
        if (seed == -1)
            seed = ((int) now_ms ()) ^ my_node_id.hashCode();
        rand = new Random (seed);
        String namespace_str = config_get_string (config, "namespace");
        MessageDigest digest = MessageDigest.getInstance("SHA");
        my_addr = new InetSocketAddress(
            my_node_id.address(), my_node_id.port());
        byte [] my_addr_bytes = RedirClient.addr2bytes(my_addr);
        my_id = RedirClient.bytes2bi(digest.digest(my_addr_bytes));
        assert ! instances.containsKey (my_id);
        instances.put (my_id, this);
        namespace = RedirClient.bytes2bi (
                digest.digest (namespace_str.getBytes ()));
        String client_stg_name =
            config_get_string (config, "client_stage_name");
        client = (RedirClient) lookup_stage (config, client_stg_name);
        long join_delay_ms = config_get_int (config, "join_delay_ms");
        if (join_delay_ms == -1)
            join_delay_ms = 0;
        acore.register_timer (join_delay_ms, ready_cb, null);
        lookup_delay_ms = config_get_int (config, "lookup_delay_ms");
        if (lookup_delay_ms == -1)
            lookup_delay_ms = 0;
    }

    public ASyncCore.TimerCB ready_cb = new ASyncCore.TimerCB () {
        public void timer_cb (Object user_data) {
            client.join (my_addr, my_id, namespace, levels,
                         10*3600, APP, join_cb, null);
        }
    };

    public RedirClient.JoinCb join_cb = new RedirClient.JoinCb () {
        public void join_cb (Object user_data) { 
            acore.register_timer (lookup_delay_ms, start_lookups_cb, null);
        }
    };

    public ASyncCore.TimerCB start_lookups_cb = new ASyncCore.TimerCB () {
        public void timer_cb (Object user_data) {
            next_lookup (); 
        }
    };

    protected static double random_exponential(double mean, Random rand) {
        double u = rand.nextDouble();
        return (0 - (mean * Math.log(1.0 - u)));
    }

    protected void next_lookup () {
        long s = Math.round (random_exponential (mean_period_ms, rand));
        acore.register_timer (s, next_lookup_cb, null);
    }

    public ASyncCore.TimerCB next_lookup_cb = new ASyncCore.TimerCB () {
        public void timer_cb (Object user_data) {
            byte[] keyb = new byte[21];
            rand.nextBytes(keyb);
            keyb[0] = 0;
            BigInteger key = RedirClient.bytes2bi(keyb);
            logger.info("looking up successor of 0x"
                        + GuidTools.guid_to_string(key));
            Long start_ms = new Long (timer_ms ());
            client.lookup(key, namespace, levels, APP, lookup_done_cb, 
                          start_ms);
            next_lookup ();
        }
    };

    public RedirClient.LookupCb lookup_done_cb = new RedirClient.LookupCb () {
        public void lookup_cb (BigInteger key, InetSocketAddress succ_addr,
                BigInteger succ_hash, int gets, Object user_data) {

            long lat_ms = timer_ms () - ((Long) user_data).longValue ();

            BigInteger expected_succ = null;
            if (sim_running) {
                expected_succ = (BigInteger) 
                    instances.tailMap (key).firstKey ();
                assert (expected_succ == null)
                    || (expected_succ.compareTo (key) != 0); 
                if ((expected_succ == null) 
                    || (expected_succ.compareTo (key) < 0)) 
                    expected_succ = (BigInteger) instances.firstKey ();
            }

            if ((! sim_running) || succ_hash.equals (expected_succ)) {
                logger.info ("for key 0x" 
                        + GuidTools.guid_to_string (key)
                        + " found successor 0x"
                        + GuidTools.guid_to_string (succ_hash)
                        + ", " + succ_addr.getAddress ().getHostAddress ()
                        + ":" + succ_addr.getPort () + " using " +
                        + gets + " gets, lat=" + lat_ms + " ms");
            }
            else {
                logger.info ("for key 0x" 
                        + GuidTools.guid_to_string (key)
                        + " found wrong successor 0x"
                        + GuidTools.guid_to_string (succ_hash)
                        + ", " + succ_addr.getAddress ().getHostAddress ()
                        + ":" + succ_addr.getPort () + ": should be 0x"
                        + GuidTools.guid_to_string (expected_succ));
            }
        }
    };

    public void handleEvent (QueueElementIF item) {
        BUG ("unexpected event: " + item);
    }
}

