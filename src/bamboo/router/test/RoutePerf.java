/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.router.test;
import org.apache.log4j.Level;
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
import ostore.util.Debug;
import ostore.util.SHA1Hash;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.EventHandlerIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import seda.sandStorm.api.SinkException;
import seda.sandStorm.api.SinkIF;
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
 * @version $Id: RoutePerf.java,v 1.2 2003/12/08 07:05:10 srhea Exp $
 */
public class RoutePerf extends bamboo.util.StandardStage 
implements SingleThreadedEventHandlerIF {

    protected static class Alarm implements QueueElementIF {}

    protected static final long app_id = 
        bamboo.router.Router.app_id (RoutePerf.class);

    protected int period;
    protected int seq;
    protected BigInteger dest;
    protected Map deliveries = new HashMap ();

    public static class Payload implements QuickSerializable {
        public int seq;
        public boolean first;
        public Payload (int s, boolean f) { seq = s; first = f; }
        public Payload (InputBuffer b) {
            seq = b.nextInt (); first = b.nextBoolean ();
        }
        public void serialize (OutputBuffer b) { b.add (seq); b.add (first); }
        public String toString () { 
            return "(" + seq + " " + (first ? "first" : "second") + ")"; 
        }
    }

    public RoutePerf () throws Exception {
        ostore.util.TypeTable.register_type (Payload.class);
	event_types = new Class [] {
            seda.sandStorm.api.StagesInitializedSignal.class,
            Alarm.class
	};
    }

    public void init (ConfigDataIF config) throws Exception {
	super.init (config);
        // logger.setLevel (Level.DEBUG);
        String dest_str = config_get_string (config, "dest");
        if (dest_str != null) {
            final String regex = "0x[0-9A-Fa-f]+";
            if (! dest_str.matches (regex)) {
                logger.fatal ("dest must match " + regex);
                System.exit (1);
            }
            dest = new BigInteger (dest_str.substring (2), 16);
            period = config_get_int (config, "period");
            if (period <= 0) {
                logger.fatal ("period must be positive");
                System.exit (1);
            }
        }
    }

    public void handleEvent (QueueElementIF item) {
        if (logger.isDebugEnabled ())
            logger.debug ("got " + item);

	if (item instanceof seda.sandStorm.api.StagesInitializedSignal) {
	    dispatch (new BambooRouterAppRegReq (
			app_id, false, false, false, my_sink));
	}
	else if (item instanceof BambooRouterAppRegResp) { 
	    BambooRouterAppRegResp resp = (BambooRouterAppRegResp) item;
            if (dest != null)
                classifier.dispatch_later (new Alarm (), period);
	}
        else if (item instanceof BambooRouteDeliver) {
            long now_ms = now_ms ();
            BambooRouteDeliver deliver = (BambooRouteDeliver) item;
            Payload payload = (Payload) deliver.payload;
            Integer seq = new Integer (payload.seq);
            Pair p = (Pair) deliveries.get (seq);
            if (p == null) {
                p = new Pair (null, null);
                deliveries.put (seq, p);
            }
            if (payload.first)
                p.first = new Long (now_ms);
            else
                p.second = new Long (now_ms);

            if (p.first != null && p.second != null) {
                deliveries.remove (seq);
                logger.info ("seq=" + seq + " inter packet time=" + 
                        (((Long) p.second).longValue () - 
                         ((Long) p.first).longValue ()) + " ms");
            }
        }
        else if (item instanceof Alarm) {
            BambooRouteInit init1 = new BambooRouteInit (
                    dest, app_id, false, false, new Payload (seq, true));
            BambooRouteInit init2 = new BambooRouteInit (
                    dest, app_id, false, false, new Payload (seq, false));
            dispatch (init1);
            dispatch (init2);
            ++seq;
            classifier.dispatch_later (new Alarm (), period);
        }
        else {
            BUG (item.toString ());
        }
    }
}

