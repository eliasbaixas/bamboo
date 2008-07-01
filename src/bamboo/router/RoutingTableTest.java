/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.router;
import java.io.*;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import ostore.security.*;
import ostore.util.*;
import ostore.util.SHA1Hash;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.EventHandlerIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import seda.sandStorm.api.SinkException;
import seda.sandStorm.api.SinkIF;
import seda.sandStorm.api.StagesInitializedSignal;
import soss.network.Network;
import bamboo.api.BambooLeafSetChanged;
import bamboo.api.BambooNeighborInfo;
import bamboo.api.BambooRouterAppRegReq;
import bamboo.api.BambooRouterAppRegResp;
import bamboo.api.BambooRoutingTableChanged;
import bamboo.api.BambooRouteInit;
import bamboo.api.BambooRouteDeliver;
import bamboo.util.GuidTools;

/**
 * Tests the quality of the routing tables, for use under the simulator only.
 *
 * @author Sean C. Rhea
 * @version $Id: RoutingTableTest.java,v 1.16 2004/02/13 04:28:01 srhea Exp $
 */
public class RoutingTableTest extends bamboo.util.StandardStage 
implements SingleThreadedEventHandlerIF {

    protected static class Alarm implements QueueElementIF {}

    protected static final long app_id = 
        bamboo.router.Router.app_id (RoutingTableTest.class);

    protected static Set all_nodes = new HashSet ();
    protected static Map guid_to_stage = new HashMap ();

    protected int GUID_DIGITS;
    protected int DIGIT_VALUES;
    protected BigInteger MODULUS;
    protected Set rt_set = new HashSet ();
    protected NeighborInfo my_ni;
    protected Network network;
    protected Set leaf_set = new HashSet ();
    protected String output_prefix;
    protected int check_period;
    protected int check_number;

    public RoutingTableTest () throws Exception {
	DEBUG = false;
	event_types = new Class [] {
            StagesInitializedSignal.class,
            Alarm.class
	};
    }

    public void init (ConfigDataIF config) throws Exception {
	super.init (config);
	int debug_level = config.getInt ("debug_level");
	if (debug_level > 0) 
	    DEBUG = true;
        int initial_check_time = config.getInt ("initial_check_time");
        if (initial_check_time < 0) initial_check_time = 60;
        initial_check_time *= 1000; // specified in seconds
        classifier.dispatch_later (new Alarm (), initial_check_time);
        check_period = config.getInt ("check_period");
        if (check_period < 0) check_period = 60;
        check_period *= 1000; // specified in seconds
        output_prefix = config.getString ("output_prefix");
        if (output_prefix == null) {
            System.err.println ("must set output_prefix in cfg file");
            System.exit (1);
        }

	if (config.getBoolean ("fake_keys")) {
	    SecureHash sh = null;
	    String fakeguid = config.getString ("fake_guid");
	    if (fakeguid != null) {
		sh = new SHA1Hash(fakeguid.getBytes(), true);
	    } 
	    else {
		int offset = config.getInt("fake_key_offset");
		if (offset != -1) {
		    sh = new SHA1Hash (my_node_id.toString()+offset);
		} 
		else {
		    sh = new SHA1Hash(my_node_id);
		}
	    }
	    my_ni = new NeighborInfo (my_node_id,
		    GuidTools.secure_hash_to_big_integer (sh));
	} 
	else {
            String keyfilename = config.getString ("pkey");
            if (keyfilename != null && !keyfilename.equals("")) {
                QSPublicKey pkey = null;
                try {
                    FileInputStream keyfile = new FileInputStream (keyfilename);
                    pkey = (QSPublicKey) QSIO.read (keyfile);
                    keyfile.close ();
                }
                catch (Exception e) {
                    Carp.die ("TClient.init:  Caught exception " + e +
                            " while trying to read pkey from disk.");
                }
                my_ni = new NeighborInfo (my_node_id,
                        GuidTools.secure_hash_to_big_integer (
                            new SHA1Hash (pkey)));
            }
            else {
                my_ni = new NeighborInfo (my_node_id,
                        GuidTools.secure_hash_to_big_integer (
                            new SHA1Hash (my_node_id)));
            }
	}

        if (my_ni == null) {
            logger.fatal ("no guid!");
            System.exit (1);
        }

        rand = new Random (my_ni.guid.hashCode ());

        System.out.println (my_ni + 
                ": adding myself to the all_nodes set.");
        all_nodes.add (my_ni);
        guid_to_stage.put (my_ni.guid, this);
    }

    protected long last_find_owner;
    protected Random rand;

    public void do_find_owner () {

        // Try to do one query every 5 seconds.

        long delay = 5000 - (now_ms () - last_find_owner);
        if (delay <= 0) 
            delay = 1;  // work around rouding error in simulator

        last_find_owner = now_ms () + delay;

        BigInteger dest = GuidTools.random_guid (rand);

        BambooRouteInit outb = new BambooRouteInit (
                dest, app_id, false, false, new QSInt (0));

        classifier.dispatch_later (outb, delay);
    }

    public void handleEvent (QueueElementIF item) {
	if (logger.isDebugEnabled ()) logger.debug ("got " + item);

	if (item instanceof StagesInitializedSignal) {
	    dispatch (new BambooRouterAppRegReq (
			app_id, true, true, false, my_sink));
            network = Network.instance ();
	}
	else if (item instanceof BambooRouterAppRegResp) { 
	    if (logger.isDebugEnabled ()) logger.debug (" got BambooRouterAppRegResp");
	    BambooRouterAppRegResp resp = (BambooRouterAppRegResp) item;
	    MODULUS = resp.modulus;
	    GUID_DIGITS = resp.guid_digits;
	    DIGIT_VALUES = resp.digit_values;
            do_find_owner ();
	}
        else if (item instanceof BambooLeafSetChanged) {
            BambooLeafSetChanged event = (BambooLeafSetChanged) item;
            for (int i = 0; i < event.preds.length; ++i) {
                NeighborInfo ni = new NeighborInfo (
                        event.preds [i].node_id, event.preds [i].guid);
                leaf_set.add (ni);
            }
            for (int i = 0; i < event.succs.length; ++i) {
                NeighborInfo ni = new NeighborInfo (
                        event.succs [i].node_id, event.succs [i].guid);
                leaf_set.add (ni);
            }
        }
        else if (item instanceof BambooRouteDeliver) {
            BambooRouteDeliver deliver = (BambooRouteDeliver) item;
            RoutingTableTest other = 
                (RoutingTableTest) guid_to_stage.get (deliver.src);

            long actual  = now_ms () - other.last_find_owner;

            // A RouteMsg with our payload is 
            //   24 bytes for the dest
            //   24 bytes for the src
            //    8 bytes for the app_id
            //    1 byte for the intermediate_upcall
            //   12 bytes for the payload
            // ----------------------------------------------
            //   93 bytes total
            //
            // (The simulator doesn't add an IP header or anything.)

            long optimal = network.route_time_ms (
                    my_ni.node_id, other.my_ni.node_id, 117);

            if (optimal > actual)
                optimal = actual;  // correct for rounding errors

            System.out.println ("STRETCH " + now_ms () + " " + 
                    actual + " " + optimal);

            other.do_find_owner ();
        }
        else if (item instanceof BambooRoutingTableChanged) {
            BambooRoutingTableChanged event = (BambooRoutingTableChanged) item;
            if (event.added != null) {
                for (int i = 0; i < event.added.length; ++i) {
                    NeighborInfo ni = new NeighborInfo (
                            event.added [i].node_id, event.added [i].guid);
                    if (logger.isDebugEnabled ()) logger.debug ("added " + ni + " to rt_set");
                    rt_set.add (ni);
                }
            }
            if (event.removed != null) {
                for (int i = 0; i < event.removed.length; ++i) {
                    NeighborInfo ni = new NeighborInfo (
                            event.removed [i].node_id, event.removed [i].guid);
                    if (logger.isDebugEnabled ()) logger.debug ("removed " + ni + " from rt_set");
                    rt_set.remove (ni);
                }
            }
        }
        else if (item instanceof Alarm) {

            PrintStream output = null;
            try {
                output = new PrintStream (new BufferedOutputStream (
                            new FileOutputStream (output_prefix + "." +
			    check_number++, true)));
            }
            catch (IOException e) {
                BUG ("caught " + e);
            }

            int good_cnt = 0;
	    if (my_ni == null) {
		logger.fatal ("no guid, rt_set.size=" + rt_set.size ());
		System.exit (1);
	    }
            RoutingTable my_rt = new RoutingTable (my_ni, 1.0, MODULUS,
		    GUID_DIGITS, DIGIT_VALUES);
            Iterator i = rt_set.iterator ();
            while (i.hasNext ()) {
                NeighborInfo ni = (NeighborInfo) i.next ();
		if (ni.equals (my_ni))
		    continue; // Tapestry puts itself in its own RT
                long rtt_ms = network.route_time_ms (
                        my_ni.node_id, ni.node_id, 0);
                //System.err.println (my_ni + " adding " + ni);
                if (my_rt.add (ni, (double) rtt_ms, true, now_ms ()) == null) {
                    /*System.err.println (my_ni + " should have been added: " + ni + 
			    " " + rtt_ms);
                    System.err.println ("contents: " + my_rt);
                    System.exit (1);*/
                }
            }
	    RoutingTable best = new RoutingTable (my_ni, 1.0, MODULUS,
		    GUID_DIGITS, DIGIT_VALUES);
            i = all_nodes.iterator ();
            while (i.hasNext ()) {
                NeighborInfo ni = (NeighborInfo) i.next ();
                long rtt_ms = network.route_time_ms (
                        my_ni.node_id, ni.node_id, 0);
                best.add (ni, (double) rtt_ms, true, now_ms ());
                if (logger.isDebugEnabled ()) logger.debug ("adding " + ni 
                        + " to best with " + rtt_ms + " latency");
            }
            //System.err.println ("rt = \n" + my_rt);
            //System.err.println ("best_rt = \n" + best);
            for (int d = 0; d < GUID_DIGITS; ++d) {
                for (int v = 0; v < DIGIT_VALUES; ++v) {
                    RoutingTable.RoutingEntry my_re = my_rt.primary_re (d, v);
                    if ((my_re != null) &&
                        my_re.ni.node_id.equals (my_ni.node_id))
                        continue;
                    RoutingTable.RoutingEntry best_re = best.primary_re (d, v);
                    if (best_re != null) {
                        if (my_re == null) {
                            if (leaf_set.contains (best_re.ni)) {
                                output.println ("RT Test | level " + d + 
                                        " good: " + my_ni + 
                                        " knows about " + 
                                        best_re.ni + " at " + best_re.rtt_ms
                                        + " (in leaf set only)");
                            }
                            else {
                                output.println ("RT Test | level " + d + 
                                        " hole: " + my_ni + 
                                        " should know about " + best_re.ni);
                                output.println ("RT Test | " + 
                                        best_re.ni + " not in leaf set=");
                                Iterator j = leaf_set.iterator ();
                                while (j.hasNext ()) {
                                    output.println (
                                            "RT Test | " + j.next ());
                                }
                            }
                        }
                        else if ((! best_re.ni.equals (my_re.ni)) &&
                                 (best_re.rtt_ms < my_re.rtt_ms)) {
                            output.println ("RT Test | level " + d + 
                                    " inefficient: " + 
                                    my_ni + " should know about " + best_re.ni +
                                    " at " + best_re.rtt_ms + " instead of " +
                                    my_re.ni + " at " + my_re.rtt_ms);
                        }
                        else {
                            if (best_re.rtt_ms > my_re.rtt_ms) {
                                logger.fatal ("Bug: best=" + 
                                        best_re + " mine=" + my_re);
                                System.exit (1);
                            }

                            output.println ("RT Test | level " + d + 
                                    " good: " + my_ni + 
                                    " knows about " + 
                                    my_re.ni + " at " + my_re.rtt_ms);
                        }
                    }
                }
            }

            output.close ();

	    classifier.dispatch_later (new Alarm (), check_period);
        }
        else {
            BUG (item.toString ());
        }
    }
}

