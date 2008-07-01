/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.router;

import bamboo.api.BambooAddToLocationCache;
import bamboo.api.BambooLeafSetChanged;
import bamboo.api.BambooNeighborInfo;
import bamboo.api.BambooReverseRoutingTableChanged;
import bamboo.api.BambooRouteContinue;
import bamboo.api.BambooRouteDeliver;
import bamboo.api.BambooRouteInit;
import bamboo.api.BambooRouteUpcall;
import bamboo.api.BambooRouterAppRegReq;
import bamboo.api.BambooRouterAppRegResp;
import bamboo.api.BambooRoutingTableChanged;
import bamboo.lss.ASyncCore;
import bamboo.lss.DuplicateTypeException;
import bamboo.lss.DustDevil;
import bamboo.lss.Network;
import bamboo.lss.Rpc;
import bamboo.dht.Dht;
import bamboo.util.GuidTools;
import bamboo.util.Pair;
import bamboo.util.StandardStage;
import bamboo.vivaldi.Vivaldi;
import bamboo.vivaldi.VirtualCoordinate;
import java.io.FileInputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.log4j.Level;
import org.apache.log4j.Level;
import ostore.network.NetworkLatencyReq;
import ostore.network.NetworkLatencyResp;
import ostore.network.NetworkMessage;
import ostore.network.NetworkMessageResult;
import ostore.security.QSPublicKey;
import ostore.util.NodeId;
import ostore.util.QSIO;
import ostore.util.QuickSerializable;
import ostore.util.SHA1Hash;
import ostore.util.SecureHash;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import seda.sandStorm.api.SinkException;
import seda.sandStorm.api.SinkIF;
import static bamboo.util.Curry.*;

/**
 * The routing and dynamic ring maintenance code for Bamboo.
 *
 * @author Sean C. Rhea
 * @version $Id: Router.java,v 1.119 2005/12/15 21:16:14 srhea Exp $
 */
public class Router extends StandardStage
implements SingleThreadedEventHandlerIF {

    protected static Map<NodeId,Router> instances = 
        new LinkedHashMap<NodeId,Router>();

    /**
     * Returns the Router stage for a given Bamboo node.
     */
    public static Router instance (NodeId nodeID) {
        return instances.get (nodeID);
    }

    /**
     * Computes an app_id based on the class name for convenience.  
     */
    public static final long app_id (Class clazz) {
        return ostore.util.ByteUtils.bytesToLong (
                (new SHA1Hash (clazz.getName ())).bytes (),
                new int [1]);
    }

    /**
     * Computes an app_id based on the class name for convenience.
     */
    public static final long applicationID(Class clazz) {
        return app_id(clazz);
    }

    protected static class ApplicationInfo {

        Thunk2<BambooNeighborInfo[],BambooNeighborInfo[]> leafSetChanged;
        Thunk2<BambooNeighborInfo[],BambooNeighborInfo[]> routingTableChanged;
        Thunk2<BambooNeighborInfo[],BambooNeighborInfo[]> 
            reverseRoutingTableChanged;
        Thunk6<BigInteger,BigInteger,NodeId,Long,Long,QuickSerializable> 
            routeUpcall;
        Thunk6<BigInteger,BigInteger,NodeId,Long,Long,QuickSerializable> 
            routeDeliver;

        ApplicationInfo(
               Thunk2<BambooNeighborInfo[],BambooNeighborInfo[]> 
               leafSetChanged,
               Thunk2<BambooNeighborInfo[],BambooNeighborInfo[]> 
               routingTableChanged,
               Thunk2<BambooNeighborInfo[],BambooNeighborInfo[]> 
               reverseRoutingTableChanged,
               Thunk6<BigInteger,BigInteger,NodeId,Long,Long,QuickSerializable> 
               routeUpcall,
               Thunk6<BigInteger,BigInteger,NodeId,Long,Long,QuickSerializable> 
               routeDeliver) {
            this.leafSetChanged = leafSetChanged;
            this.routingTableChanged = routingTableChanged;
            this.reverseRoutingTableChanged = reverseRoutingTableChanged;
            this.routeUpcall = routeUpcall;
            this.routeDeliver = routeDeliver;
        }
    }

    protected Map<Long,ApplicationInfo> apps = 
        new LinkedHashMap<Long,ApplicationInfo>();

    /**
     * Thrown if {@link #registerApplication} is called with a duplicate
     * application ID.
     */
    public static class DuplicateApplicationException extends Exception {
        public DuplicateApplicationException(String msg) { super(msg); }
    };

    /**
     * Register an application (such as bamboo.dht.Dht) to use the Router.
     * 
     * @param id A value to identify this application in route messages sent
     * across the network.
     *
     * @param leafSetChanged Called when the leaf set changes.  The first
     * array is the new predecessor list; the second is the new successor
     * list.
     *
     * @param routingTableChanged Called when the routing table changes.  The
     * first array is the nodes added to the routing table; the second is
     * those removed from the routing table.
     *
     * @param reverseRoutingTableChanged Called when we are added to or
     * removed from some other node's routing table.  The first array is the
     * nodes added to the routing table; the second is those removed from the
     * routing table.
     *
     * @param routeUpcall Called when a message is routed through this node,
     * and the source specified that it should upcall at nodes that are not
     * the root of the destination ID.  The arguments are source ID,
     * destination ID, immediate source IP:port, the queuing time before the
     * message was forwarded, the estimated round trip time from the immediate
     * source to this node, and the payload.
     *
     * @param routeDeliver Called when we are the root for a route message.
     * The arguments are source ID, destination ID, immediate source IP:port,
     * the queuing time before the message was forwarded, the estimated round
     * trip time from the immediate source to this node, and the payload.
     *
     * @throws DuplicateApplicationException if
     * <code>registerApplication</code> has already been called with this
     * <code>id</code>
     */
    public void registerApplication(long id,
            Thunk2<BambooNeighborInfo[],BambooNeighborInfo[]> leafSetChanged,
            Thunk2<BambooNeighborInfo[],BambooNeighborInfo[]> 
            routingTableChanged,
            Thunk2<BambooNeighborInfo[],BambooNeighborInfo[]> 
            reverseRoutingTableChanged,
            Thunk6<BigInteger,BigInteger,NodeId,Long,Long,QuickSerializable>
            routeUpcall,
            Thunk6<BigInteger,BigInteger,NodeId,Long,Long,QuickSerializable>
            routeDeliver) 
            throws DuplicateApplicationException {
        if (apps.containsKey(new Long(id)))
            throw new DuplicateApplicationException("duplicate id " + id);
        apps.put(new Long(id), new ApplicationInfo(leafSetChanged, 
                    routingTableChanged, reverseRoutingTableChanged, 
                    routeUpcall, routeDeliver));
    }

    /**
     * The identifier of this node.
     */
    public BigInteger id() { return my_guid; }

    /**
     * One larger than the largest identifier supported by the system.
     */
    public BigInteger modulus() { return MODULUS; }

    /**
     * The number of digits per identifier being used by this router.
     */
    public int digitsPerID() { return GUID_DIGITS; }

    /**
     * The number of values per digit in each identifier being used by this
     * router.
     */
    public int valuesPerDigit() { return DIGIT_VALUES; }

    /**
     * Initiate a routing operation to <code>dest</code>.  Place the message
     * to be sent in <code>payload</code>.  To receive the message, the node
     * responsible for <code>dest</code> must have registered an application
     * (see <code>registerApplication</code>) with the given
     * <code>applicationID</code>.  If <code>intermediateUpcall</code> is
     * true, a the <code>routeUpcall</code> function object given in that call
     * to <code>registerApplication</code> will be called on each intermediate
     * node in the path.  To continue the routing operation, that node must
     * call <code>routeContinue</code>.  Finally, the
     * <code>routeDeliver</code> function object given in the call to
     * <code>registerApplication</code> will be called once the message
     * reaches the node responsible for <code>dest</code>.
     */
    public void routeInit(final BigInteger dest, final long applicationID, 
            final boolean intermediateUpcall, final QuickSerializable payload) {

        if (! apps.containsKey(new Long(applicationID))) {
            logger.error("routeInit called with unknown application ID " 
                         + applicationID);
            System.exit(1);
        }

        NeighborInfo next_hop = calc_next_hop (dest, true);
        if (next_hop == my_neighbor_info) {
            deliver(my_guid, dest, my_node_id, applicationID, 1,
                    0L, 0L, payload);
        }
        else {
            RouteMsg outb = new RouteMsg (next_hop.node_id,
                    my_guid, dest, applicationID, intermediateUpcall,
                    my_guid, payload);
            outb.comp_q = my_sink;
            outb.user_data = new RecursiveRouteCB (next_hop, new Runnable() {
                    public void run() { 
                        routeInit(dest, applicationID, 
                                  intermediateUpcall, payload); 
                        }
                    });
            if (no_rexmit_routes)
                outb.timeout_sec = -1;
            else
                outb.timeout_sec = 5;
            dispatch (outb);
        }
    }

    /**
     * Coninue a routing operation after an upcall.  See the comments for
     * {@link #routeInit} for more information.
     */
    public void routeContinue(final BigInteger src, final BigInteger dest, 
                final NodeId immediateSource, final long applicationID, 
                final boolean intermediateUpcall, 
                final QuickSerializable payload) {

        if (! apps.containsKey(new Long(applicationID))) {
            logger.error("routeContinue called with unknown application ID " 
                    + applicationID);
            System.exit(1);
        }

        NeighborInfo next_hop = calc_next_hop (dest, true);
	if (next_hop == my_neighbor_info) {
	    deliver (src, dest, immediateSource,
		     applicationID, 1, 0L, 0L, payload);
	}
	else {
	    RouteMsg outb = new RouteMsg (next_hop.node_id,
		    src, dest, applicationID, intermediateUpcall,
		    my_guid, payload);
            outb.comp_q = my_sink;
            outb.user_data = new RecursiveRouteCB (next_hop, new Runnable() {
                    public void run() { 
                        routeContinue(src, dest, immediateSource, 
                                      applicationID, intermediateUpcall, 
                                      payload); 
                        }
                    });
            if (no_rexmit_routes)
                outb.timeout_sec = -1;
            else
                outb.timeout_sec = 5;
	    dispatch (outb);
	}
    }

    /**
     * Treat this as read-only or perish.
     */
    public LeafSet leafSet() {
        return leaf_set;
    }

    /**
     * Treat this as read-only or perish.
     */
    public RoutingTable routingTable() {
        return rt;
    }

    /**
     * The modulus of the ring of identifiers.  If you want to get at this
     * value, get it from BambooRouterAppRegResp.
     */
    protected BigInteger MODULUS;

    /**
     * The number of digits in each identifier.  If you want to get at this
     * value, get it from BambooRouterAppRegResp.
     */
    protected int GUID_DIGITS;

    protected int GUID_BITS;

    /**
     * The number of possible distinct values for each digit.  If you want
     * to get at this value, get it from BambooRouterAppRegResp.
     */
    protected int DIGIT_VALUES;

    protected int DIGIT_BITS;

    protected static final int PING_ITER = 10;

    protected boolean PNS;
    protected boolean ignore_possibly_down;
    protected boolean no_rexmit_routes;

    protected Map<NeighborInfo,Double> latency_map = 
        new LinkedHashMap<NeighborInfo,Double>();

    protected Map<NeighborInfo,Long> possibly_down = 
        new LinkedHashMap<NeighborInfo,Long>();
    protected Map<NodeId,NeighborInfo> possibly_down_helper = 
        new LinkedHashMap<NodeId,NeighborInfo>();
    
    public Set<NeighborInfo> possiblyDown() {
        return possibly_down.keySet();
    }

    public void addToPossiblyDown(NodeId n) {
        // This is horribly inefficient, but we only call it on a message
        // timeout, so it's not that bad.
        for (NeighborInfo ni : leaf_set.as_list()) {
            if (ni.node_id.equals(n)) {
                addToPossiblyDown(ni);
                return;
            }
        }
        for (NeighborInfo ni : rt.as_list()) {
            if (ni.node_id.equals(n)) {
                addToPossiblyDown(ni);
                return;
            }
        }
    }

    public void addToPossiblyDown(NeighborInfo ni) {
        // If the node is in our leaf set, routing table, or reverse
        // routing table, note that it is temporarily down, and start
        // pinging it to see if it comes back up.

        if (leaf_set.contains (ni) || rt.contains (ni)
                || reverse_rt.contains (ni)) {

            if (! possibly_down.containsKey (ni)) {
                possibly_down.put (ni, new Long (now_ms ()));
                possibly_down_helper.put (ni.node_id, ni);
                if (logger.isDebugEnabled ()) logger.debug (
                        "added " + ni + " to possibly down");
                PingMsg outb = new PingMsg (ni.node_id);
                outb.comp_q = my_sink;
                outb.user_data = new SecondChancePingCB (ni);
                outb.timeout_sec = 60;
                dispatch (outb);
            }
        }
    }

    public void removeFromPossiblyDown(NeighborInfo ni) {
        if (possibly_down.containsKey (ni)) {
            possibly_down.remove (ni);
            possibly_down_helper.remove(ni.node_id);
            if (logger.isDebugEnabled ()) logger.debug (
                    "removed " + ni + " from possibly_down");
        }
    }

    public void removeFromPossiblyDown(NodeId n) {
        // Unlike addToPossiblyDown, we call this function on every successful
        // send, so it needs to be more efficient.  So we use the
        // possibly_down_helper map to go from NodeId to NeighborInfo quickly.
        NeighborInfo ni = possibly_down_helper.get(n);
        if (ni != null) 
            removeFromPossiblyDown(ni);
    }

    protected Set<NeighborInfo> periodic_pings = 
        new LinkedHashSet<NeighborInfo>();

    protected BigInteger my_guid;
    protected int [] my_digits;
    protected NeighborInfo my_neighbor_info;

    protected boolean initialized;
    protected LinkedList<QueueElementIF> waitq = 
        new LinkedList<QueueElementIF>();

    protected LocationCache location_cache;
    protected Set<NodeId> down_nodes = new LinkedHashSet<NodeId>();
    protected int down_nodes_cap = 20;
    protected boolean immediate_join;

    protected LeafSet leaf_set;
    protected int leaf_set_size;

    protected RoutingTable rt;

    protected LinkedList<NodeId> gateways = new LinkedList<NodeId>();

    protected static class AppData {
	public AppData (SinkIF s, boolean ls, boolean rt, boolean rrt) {
	    sink = s;
	    want_leaf_set_updates = ls;
	    want_routing_table_updates = rt;
	    want_reverse_routing_table_updates = rrt;
	}
	public SinkIF sink;
	public boolean want_leaf_set_updates;
	public boolean want_routing_table_updates;
	public boolean want_reverse_routing_table_updates;
    }

    protected Random rand;

    protected int periodic_ping_period;
    protected int partition_check_alarm_period;
    protected int ls_alarm_period;
    protected int near_rt_alarm_period;
    protected int far_rt_alarm_period;
    protected int lookup_rt_alarm_period;
    protected boolean pastry_mode = false;

    protected long start_time_ms;

    protected Set<NeighborInfo> reverse_rt = new LinkedHashSet<NeighborInfo>();

    protected Network network;
    protected Vivaldi vivaldi;
    protected Rpc rpc;

    public Router () throws Exception {

        // Application ID 0 is reserved for the router's use.
        registerApplication(0, null, null, null, null, null); 
                
	ostore.util.TypeTable.register_type (LookupReqPayload.class);
	ostore.util.TypeTable.register_type (NeighborInfo.class);
        ostore.util.TypeTable.register_type(CoordReq.class);
        ostore.util.TypeTable.register_type(CoordResp.class);

	event_types = new Class [] {
	    BambooAddToLocationCache.class,
            BambooRouteContinue.class,
	    BambooRouteInit.class,
	    BambooRouterAppRegReq.class,
        };
	inb_msg_types = new Class [] {
	    PingMsg.class,
	    JoinReq.class,
	    JoinResp.class,
	    LeafSetReq.class,
	    LeafSetChanged.class,
            LookupRespMsg.class,
	    RouteMsg.class,
	    RoutingTableReq.class,
	    RoutingTableResp.class,
	    RoutingNeighborAnnounce.class
	};
    }

    protected int config_get_seconds (
            ConfigDataIF config, String name, int default_value) {
        return configGetInt(config, name, default_value) * 1000;
    }

    public void init (ConfigDataIF config) throws Exception {
	super.init (config);

        acore.registerTimer(0, ready);

        no_rexmit_routes =
            config_get_boolean (config, "no_rexmit_routes");
	ignore_possibly_down =
            config_get_boolean (config, "ignore_possibly_down");
	PNS = ! config_get_boolean (config, "ignore_proximity");

	GUID_BITS = 160;
	MODULUS = BigInteger.valueOf (2).pow (GUID_BITS);
	DIGIT_VALUES = configGetInt(config, "digit_values", 16);
	if (DIGIT_VALUES <= 1)
	    throw new IllegalArgumentException ("digit_values must be >= 0");

	{
	    DIGIT_BITS = 1;
	    int i = 2;
	    while (i < DIGIT_VALUES) {
		++DIGIT_BITS; i <<= 1;
	    }
	    if (i != DIGIT_VALUES)
		throw new IllegalArgumentException (
			"digit_values must be a power of two");

	    if (GUID_BITS % DIGIT_BITS != 0)
		throw new IllegalArgumentException (
			"log_2 (digit_values) must divide 160");

	    GUID_DIGITS = GUID_BITS / DIGIT_BITS;
	}

        // Cost estimates:
        //
        // IP header                                    20 bytes
        // UdpCC header (plus type code)                24 bytes
        // NodeId                                        7 bytes
        // BigInteger (guid)                            24 bytes
        // PingMsg                                      44 bytes
        // LeafSetChanged (with 8 neighbors)           317 bytes
        // RoutingTableReq                              72 bytes
        // EmbeddedRoutingTableReq (+RouteMsg)         144 bytes
        // RoutingTableResp (for 16 digit values)      544 bytes
        //
        // Msgs are ACKed with pings, so they take an additional 28 bytes
        // each.  In req-resp pairs, the ACK for the req is piggy-backed on
        // the resp, but the resp ACK goes alone.
        //
        // log_16 1000 = 2.5
        //
        // Assuming a 1000-node network:
        //
        // Each                                        We send
        // ----                                       ----------
        // ping_alarm_period                          3456 bytes
        // ls_alarm_period                             690 bytes
        // near_rt_alarm_period                        616 bytes
        // far_rt_alarm_period                        1002 bytes
        //
        // With default values, that works out to:
        //
        // ping_alarm_period                       691.2 bytes/s
        // ls_alarm_period                         659.0 bytes/s
        // near_rt_alarm_period                    123.2 bytes/s
        // far_rt_alarm_period                     100.2 bytes/s
        // -----------------------------------------------------
        // total                                  1573.6 bytes/s
        //
        // Finally, pings will not be sent if another message was sent
        // instead, but I expect that to be a small savings.

        partition_check_alarm_period =
            config_get_seconds (config, "partition_check_alarm_period", 60);
	periodic_ping_period =
            config_get_seconds (config, "periodic_ping_period",         20);
	ls_alarm_period =
            config_get_seconds (config, "ls_alarm_period",               4);
	near_rt_alarm_period =
            config_get_seconds (config, "near_rt_alarm_period",         10);
	far_rt_alarm_period =
            config_get_seconds (config, "far_rt_alarm_period",          20);
	lookup_rt_alarm_period =
            config_get_seconds (config, "lookup_rt_alarm_period",        0);

	String explicit_guid = config_get_string (config, "explicit_guid");
	String keyfilename = config_get_string (config, "pkey");
	if (keyfilename != null && !keyfilename.equals("")) {
	    QSPublicKey pkey = null;
	    try {
		FileInputStream keyfile = new FileInputStream (keyfilename);
		pkey = (QSPublicKey) QSIO.read (keyfile);
		keyfile.close ();
	    }
	    catch (Exception e) {
                logger.fatal ("Caught exception " + e +
			" while trying to read pkey (" + keyfilename +
			  ") from disk.");
                System.exit (1);
	    }
	    SecureHash my_guid_sh = new SHA1Hash (pkey);
            my_guid = GuidTools.secure_hash_to_big_integer (my_guid_sh);
	}
	else if (explicit_guid != null && !explicit_guid.equals ("")) {
            final String regex = "0x[0-9a-fA-F]+";
            if (! explicit_guid.matches (regex)) {
                logger.fatal ("explicit_guid must match " + regex);
                System.exit (1);
            }
            my_guid = new BigInteger (explicit_guid.substring (2), 16);
        }
	else {
	    SecureHash my_guid_sh = new SHA1Hash (my_node_id);
            my_guid = GuidTools.secure_hash_to_big_integer (my_guid_sh);
	}

	my_neighbor_info = new NeighborInfo (my_node_id, my_guid);
        // routers.put (my_guid, this);

	int gateway_count = config_get_int (config, "gateway_count");
        if (gateway_count < 0) {
            NodeId g = new NodeId (config_get_string (config, "gateway"));
            gateways.addLast (g);
        }
        else {
            for (int i = 0; i < gateway_count; ++i) {
                String nid_str = config_get_string (config, "gateway_" + i);
                NodeId g = null;
                try {
                    g = new NodeId (nid_str);
                }
                catch (java.net.UnknownHostException e) {
                    logger.warn ("cannot DNS resolve host: " + nid_str);
                    continue;
                }
                if (! gateways.contains (g))
                    gateways.addLast (g);
            }
        }
        boolean am_gateway = false;
        for (Iterator i = gateways.iterator (); i.hasNext (); ) {
            if (i.next ().equals (my_node_id)) {
                am_gateway = true;
                i.remove ();
            }
        }

        if (gateways.isEmpty ()) {
            if (am_gateway && ((gateway_count == 1) || (gateway_count < 0))) {
                // We are the first node in the network.  All is well.
            }
            else {
                logger.fatal ("Could not DNS resolve any gateways.");
                System.exit (1);
            }
        }

	logger.info ("Bamboo node " + my_node_id + " has guid 0x" +
		GuidTools.guid_to_string (my_guid));

	leaf_set_size = configGetInt(config, "leaf_set_size", 2);
	leaf_set = new LeafSet (my_neighbor_info, leaf_set_size, MODULUS);

        double rt_scale = config_get_double (config, "rt_scale");
        if (rt_scale == -1.0)
            rt_scale = 0.9;
	rt = new RoutingTable (my_neighbor_info, rt_scale, MODULUS,
		GUID_DIGITS, DIGIT_VALUES);
        my_digits = rt.guid_to_digits (my_guid);

	rand = new Random (my_guid.longValue ());

        location_cache = new LocationCache (
                configGetInt(config, "location_cache_size", 0), MODULUS);

        // If immediate_join=true in the cfg file, we add all gateways to the
        // down_nodes set and join immediately (through ourselves).  This hack
        // allows all PlanetLab nodes to have the same set of 10 or so
        // gateways and to be started in any order.

        immediate_join = config_get_boolean (config, "immediate_join");
        if (immediate_join) {
            down_nodes_cap = Math.min (down_nodes_cap, gateways.size ());
            for (Iterator i = gateways.iterator (); i.hasNext (); ) {
                NodeId n = (NodeId) i.next ();
                add_to_down_nodes (n);
                i.remove ();
            }
        }

        // Done initializing.  Make ourselves available.
        instances.put (my_node_id, this);
    }

    public void handleEvent (QueueElementIF item) {

	if (logger.isDebugEnabled ())
	    logger.debug ("got " + item);

	if (item instanceof PingMsg) {
            // Ignore it; the network-level ACK is all that's important.
        }
	else if (item instanceof JoinResp) {
	    handle_join_resp ((JoinResp) item);
	}
	else if (item instanceof RoutingNeighborAnnounce) {
	    handle_rt_annc ((RoutingNeighborAnnounce) item);
	}
        else if (item instanceof NetworkMessageResult) {
            handle_net_msg_result ((NetworkMessageResult) item);
        }
        else if (item instanceof NetworkLatencyResp) {
            handle_net_lat_resp ((NetworkLatencyResp) item);
        }
	else {
	    if (initialized) {
		if (item instanceof JoinReq) {
		    handle_join_req ((JoinReq) item);
		}
		else if (item instanceof LeafSetReq) {
		    handle_leaf_set_req ((LeafSetReq) item);
		}
		else if (item instanceof LeafSetChanged) {
		    handle_leaf_set_changed ((LeafSetChanged) item);
		}
		else if (item instanceof RoutingTableReq) {
		    handle_routing_table_req ((RoutingTableReq) item);
		}
		else if (item instanceof RoutingTableResp) {
		    handle_routing_table_resp ((RoutingTableResp) item);
		}
		else if (item instanceof BambooRouterAppRegReq) {
		    handle_router_app_reg_req ((BambooRouterAppRegReq) item);
		}
		else if (item instanceof BambooRouteInit) {
		    handle_route_init ((BambooRouteInit) item);
		}
		else if (item instanceof BambooAddToLocationCache) {
		    BambooAddToLocationCache req =
                        (BambooAddToLocationCache) item;
                    NeighborInfo ni = new NeighborInfo (req.node_id, req.guid);
                    location_cache.add_node (ni);
		}
		else if (item instanceof BambooRouteContinue) {
		    handle_route_continue ((BambooRouteContinue) item);
		}
		else if (item instanceof RouteMsg) {
		    handle_route_msg ((RouteMsg) item);
		}
		else if (item instanceof LookupRespMsg) {
		    handle_lookup_resp_msg ((LookupRespMsg) item);
		}
		else {
		    throw new IllegalArgumentException ("unknown event type "
                            + item.getClass ().getName ());
		}
	    }
	    else {
		waitq.addLast (item);
	    }
	}
    }

    protected interface NetMsgResultCB {
        void success ();
        void failure ();
    }

    protected void handle_net_msg_result (NetworkMessageResult result) {
        NetMsgResultCB cb = (NetMsgResultCB) result.user_data;
        if (result.success)
            cb.success ();
        else
            cb.failure ();
    }

    protected class PeriodicPingCB implements NetMsgResultCB {
        NeighborInfo ni;
        public PeriodicPingCB (NeighborInfo n) { ni = n; }
        public void success () {
            periodic_pings.remove (ni);
            generic_msg_success (ni);
            dispatch (new NetworkLatencyReq (ni.node_id, my_sink, ni));
        }
        public void failure () {
            periodic_pings.remove (ni);
            generic_msg_failure (ni, null);
        }
        public String toString () { return "(PeriodicPingCB " + ni + ")"; }
    }

    protected class SecondChancePingCB implements NetMsgResultCB {
        NeighborInfo ni;
        public SecondChancePingCB (NeighborInfo n) { ni = n; }
        public void success () { generic_msg_success (ni); }
        public void failure () { handle_monitor_node_down (ni); }
        public String toString () { return "(SecondChancePingCB " + ni + ")"; }
    }

    protected class RecursiveRouteCB  implements NetMsgResultCB {
        NeighborInfo ni;
        Runnable retry;
        BigInteger dest;
        long start_time;

        public RecursiveRouteCB (NeighborInfo n, Runnable r) {
            ni = n; retry = r;
        }
        public void success () { 
            generic_msg_success (ni); 
        }
        public void failure () { 
            generic_msg_failure (ni, retry); 
        }
        public String toString () {
            return "(RecursiveRouteCB " + ni + " " + retry + ")";
        }
    }

    protected long randomPeriod(int mean) {
        return mean / 2 + rand.nextInt(mean);
    }

    protected Runnable pingAlarm = new Runnable() {
        public void run() {
            for (int i = 0; i < 3; ++i) {
                Iterator j = null;

                if (i == 0)      j = leaf_set.as_list ().iterator ();
                else if (i == 1) j = rt.as_list ().iterator ();
                else             j = reverse_rt.iterator ();

                while (j.hasNext ()) {
                    NeighborInfo ni = (NeighborInfo) j.next ();
                    if ((! periodic_pings.contains (ni) &&
                                (! possibly_down.containsKey (ni)))) {
                        periodic_pings.add (ni);
                        PingMsg outb = new PingMsg (ni.node_id);
                        outb.comp_q = my_sink;
                        outb.user_data = new PeriodicPingCB (ni);
                        outb.timeout_sec = 5;
                        dispatch (outb);
                    }
                }
            }

            acore.registerTimer(randomPeriod(periodic_ping_period), pingAlarm);
        }
    };

    protected void use_as_periodic_ping (NeighborInfo ni, NetworkMessage outb) {
        if (! periodic_pings.contains (ni)) {
            periodic_pings.add (ni);
            outb.comp_q = my_sink;
            outb.user_data = new PeriodicPingCB (ni);
            outb.timeout_sec = 5;
        }
    }

    protected void generic_msg_success (NeighborInfo ni) {
        removeFromPossiblyDown(ni);
        if (down_nodes.remove (ni.node_id)) {
            if (logger.isDebugEnabled ()) logger.debug (
                    "removed " + ni.node_id + " from down_nodes");
        }
    }

    protected void generic_msg_failure (NeighborInfo ni, Runnable retry) {
        if (logger.isDebugEnabled ()) 
            logger.debug ("failed on message send to " + ni + ".");

        // Immediately take the node of the location cache.

        if (location_cache.remove_node (ni)) {
            if (logger.isDebugEnabled ()) logger.debug (
                    "taking " + ni + ".  Out of location cache.");
        }

        addToPossiblyDown(ni);

        if (retry != null)
            retry.run();
    }

    protected void handle_rt_annc (RoutingNeighborAnnounce annc) {
	NeighborInfo ni = new NeighborInfo (annc.peer, annc.guid);
	if (annc.add)
	    add_to_rrt (ni);
	else
	    remove_from_rrt (ni);
    }

    protected void handle_monitor_node_down (NeighborInfo ni) {

	if (logger.isDebugEnabled ()) logger.debug (ni + " is down.");

        add_to_down_nodes (ni.node_id);

        if (latency_map.containsKey (ni))
            latency_map.remove (ni);

	// Take it out of our leaf set...

	boolean removed_ls = remove_from_ls (ni);

	// ...our routing table...

	boolean removed_rt = remove_from_rt (ni);

        if (logger.isInfoEnabled () && (removed_ls || removed_rt)) {
            StringBuffer buf = new StringBuffer (95);
            buf.append ("neighbor ");
            buf.append (ni.node_id.address ().getHostAddress ());
            buf.append (":");
            buf.append (ni.node_id.port ());
            buf.append (" unreachable; removed it from");
            if (removed_ls)
                buf.append (" leaf set");
            if (removed_rt) {
                if (removed_ls)
                    buf.append (" and routing table");
                else
                    buf.append (" routing table");
            }
            logger.info (buf);
        }

	// ...and the reverse routing table.

	boolean removed_rrt = remove_from_rrt (ni);

	if (logger.isDebugEnabled ()
                && (! (removed_ls || removed_rt || removed_rrt))) {
	    logger.debug (ni + " is down; but is no " +
		    "longer in our leaf set, or routing table, or reverse rt.");
	}
    }

    protected class PartitionCheckCB implements NetMsgResultCB {
        public NodeId node_id;
        public PartitionCheckCB (NodeId n) { node_id = n; }
        public void success () {
            // So that we don't keep looking for a partition through a node
            // that's in our current partition.
            if (logger.isDebugEnabled ())
                logger.debug ("down_node " + node_id + " is up.");
            if (down_nodes.remove (node_id)) {
                if (logger.isDebugEnabled ()) logger.debug (
                        "removed " + node_id + " from down_nodes");
            }
        }
        public void failure () {}
    }

    protected Runnable partitionCheckAlarm = new Runnable() {
        public void run() {
            if (down_nodes.size () > 0) {
                int which = rand.nextInt (down_nodes.size ());
                Iterator<NodeId> i = down_nodes.iterator ();
                NodeId n = null;
                while (which-- >= 0)
                    n = i.next ();

                JoinReq outb = new JoinReq (n, my_node_id, my_guid, 0);
                outb.timeout_sec = 10;
                outb.comp_q = my_sink;
                outb.user_data = new PartitionCheckCB (n);
                if (logger.isDebugEnabled ()) logger.debug (
                        "sending " + outb + " to check for partition");
                dispatch (outb);
            }
            else {
                if (logger.isDebugEnabled ()) logger.debug ("no down nodes");
            }

            if (partition_check_alarm_period != 0) {
                acore.registerTimer(
                        randomPeriod(partition_check_alarm_period), this);
            }
        }
    };

    protected Runnable leafSetAlarm = new Runnable() {
        public void run() {
            // Occasionally send an unprovoked LS changed message to
            // one of our leaf set members.

            NeighborInfo ni = leaf_set.random_member (rand);

            // Try not to slam nodes that are already behind.
            if ((ni != null) && (! possibly_down.containsKey (ni))) {
                LeafSetChanged outb =
                    new LeafSetChanged(ni.node_id, my_guid, leaf_set.as_list());
                use_as_periodic_ping (ni, outb);
                outb.want_reply = true;
                dispatch (outb);
            }
            acore.registerTimer(randomPeriod(ls_alarm_period), this);
        }
    };

    protected void handle_leaf_set_req (LeafSetReq req) {
	dispatch (new LeafSetChanged (req.peer, my_guid, leaf_set.as_list ()));
    }

    protected void handle_leaf_set_changed (LeafSetChanged msg) {

	NeighborInfo sender = new NeighborInfo (msg.peer, msg.guid);
        location_cache.add_node (sender);

	// For each person in the sender's leaf set, if they are not in our
	// leaf set, and possibly should be, try to add them.

	for (Iterator i = msg.leaf_set.iterator (); i.hasNext (); ) {
	    NeighborInfo other = (NeighborInfo) i.next ();
            // Don't add nodes to the location cache w/o direct confirmation
            // that they're up, such as receiving a message from them.
            // location_cache.add_node (other);
	    if (leaf_set.promising (other)) {
                add_to_ls (other);
	    }
	    else if (logger.isDebugEnabled ())
                logger.debug (other + " isn't promising.");
	}

	// Same goes for the sender.

	if (leaf_set.promising (sender))
            add_to_ls (sender);

	// Respond in kind.

	if (msg.want_reply) {
            LeafSetChanged outb = new LeafSetChanged (
                    sender.node_id, my_guid, leaf_set.as_list ());
            use_as_periodic_ping (sender, outb);
            dispatch (outb);
	}
    }

    protected void handle_router_app_reg_req (BambooRouterAppRegReq req) {

        final SinkIF sink = req.completion_queue;
        final long applicationID = req.app_id;

        final Thunk2<BambooNeighborInfo[],BambooNeighborInfo[]> lsct = 
            !req.send_leaf_sets ? null : 
            new Thunk2<BambooNeighborInfo[],BambooNeighborInfo[]>() {
                public void run(BambooNeighborInfo preds[], 
                        BambooNeighborInfo succs[]) {
                    application_enqueue(sink, 
                            new BambooLeafSetChanged(preds, succs));
                }
            };

        final Thunk2<BambooNeighborInfo[],BambooNeighborInfo[]> rtct = 
            !req.send_rt ? null :
            new Thunk2<BambooNeighborInfo[],BambooNeighborInfo[]>() {
                public void run(BambooNeighborInfo added[],
                                BambooNeighborInfo removed[]) {
                    application_enqueue(sink, 
                            new BambooRoutingTableChanged (added, removed));
                }
            };

        final Thunk2<BambooNeighborInfo[],BambooNeighborInfo[]> rrtct = 
            !req.send_reverse_rt ? null :
            new Thunk2<BambooNeighborInfo[],BambooNeighborInfo[]>() {
                public void run(BambooNeighborInfo added[],
                                BambooNeighborInfo removed[]) {
                    application_enqueue(sink, 
                        new BambooReverseRoutingTableChanged(added, removed));
                }
            };

        Thunk6<BigInteger,BigInteger,NodeId,Long,Long,QuickSerializable> rut = 
        new Thunk6<BigInteger,BigInteger,NodeId,Long,Long,QuickSerializable>() {
            public void run(BigInteger src, BigInteger dest, 
                            NodeId immediateSource, Long waitMillis, 
                            Long estRTTMillis, QuickSerializable payload) {
		application_enqueue(sink, new BambooRouteUpcall(
                            src, dest, immediateSource, applicationID,
                            false /* not iterative */, 0 /* tries */,
                            waitMillis.longValue(), 
                            estRTTMillis.longValue(), payload));
            }
        };

        Thunk6<BigInteger,BigInteger,NodeId,Long,Long,QuickSerializable> rdt = 
        new Thunk6<BigInteger,BigInteger,NodeId,Long,Long,QuickSerializable>() {
            public void run(BigInteger src, BigInteger dest, 
                            NodeId immediateSource, Long waitMillis, 
                            Long estRTTMillis, QuickSerializable payload) {
		application_enqueue(sink, new BambooRouteDeliver (
                            src, dest, immediateSource, applicationID,
                            0 /* tries */, waitMillis.longValue(), 
                            estRTTMillis.longValue(), payload));
            }
        };

        try {
            registerApplication(applicationID, lsct, rtct, rrtct, rut, rdt);
        }
        catch (DuplicateApplicationException e) {
            application_enqueue(sink, new BambooRouterAppRegResp(
                    applicationID, false, "duplicate app_id " + applicationID));
            return;
        }

        if (logger.isDebugEnabled ()) logger.debug(
                "registered app " + Long.toHexString(applicationID));

        final ApplicationInfo appInfo = apps.get(new Long(applicationID));
        assert appInfo != null;

        // Return from this function before calling the leaf set changed and
        // routing table changed functions.

        acore.registerTimer(0, new Runnable() {
            public void run() {

                notify_leaf_set_changed (appInfo);

                BambooNeighborInfo added[] = new BambooNeighborInfo[rt.size()];
                int j = 0;
                for (int level = 0; level < GUID_DIGITS; ++level) {
                    for (int digit = 0; digit < DIGIT_VALUES; ++digit) {
                        NeighborInfo ni = rt.primary (level, digit);
                        if ((ni != null) && (! ni.equals (my_neighbor_info))) {
                            added [j++] = new BambooNeighborInfo (
                                    ni.node_id, ni.guid, rtt_ms (ni));
                        }
                    }
                }
                if (rtct != null)
                    rtct.run(added, null);

                added = new BambooNeighborInfo [reverse_rt.size ()];
                j = 0;
                for (Iterator i = reverse_rt.iterator (); i.hasNext (); ) {
                    NeighborInfo ni = (NeighborInfo) i.next ();
                    added [j++] = new BambooNeighborInfo (ni.node_id, ni.guid);
                }
                if (rrtct != null)
                    rrtct.run(added, null);
            }
        });

        application_enqueue(sink, new BambooRouterAppRegResp(
                    applicationID, true, modulus(), digitsPerID(),
                    valuesPerDigit(), id()));
    }

    protected void application_enqueue (SinkIF sink, QueueElementIF item) {
	if (logger.isDebugEnabled ())
            logger.debug ("application enqueue " + item);
	try {
	    sink.enqueue (item);
	}
	catch (SinkException e) {
	    logger.warn ("Could not enqueue " + item + " b/c of " + e);
	}
    }

    protected void deliver (BigInteger src, BigInteger dest, NodeId is,
	    long app_id, int tries, long wait_ms, long est_rtt_ms,
            QuickSerializable payload) {

        if (app_id == 0) {
            if (payload instanceof LookupReqPayload) {
                LookupReqPayload req = (LookupReqPayload) payload;
                NeighborInfo peer_ni = new NeighborInfo (req.rtn_addr, src);
                location_cache.add_node (peer_ni);
                dispatch (new LookupRespMsg (req.rtn_addr, dest, my_guid));
            }
            else {
                if (logger.isDebugEnabled ())
                    logger.debug ("unknown payload type " +
                        payload.getClass ().getName ());
            }
        }
        else {
            ApplicationInfo appInfo = apps.get(new Long(app_id));
            if (appInfo == null) {
                if (logger.isDebugEnabled ())
                    logger.debug ("no app with id " + app_id);
            }
            else if (appInfo.routeDeliver != null) {
                appInfo.routeDeliver.run(src, dest, is, new Long(wait_ms),
                                         new Long(est_rtt_ms), payload);
            }
        }
    }

    protected void handle_route_continue (BambooRouteContinue req) {
	if (req.iter) {
            BUG("iterative routing no longer supported");
	}
        routeContinue(req.src, req.dest, req.immediate_src,
                      req.app_id, req.intermediate_upcall, req.payload);
    }

    protected void handle_route_init (BambooRouteInit req) {
	if (req.iter)
            BUG("iterative routing no longer supported");
        routeInit(req.dest, req.app_id, req.intermediate_upcall, req.payload);
    }

    protected void handle_route_msg (final RouteMsg req) {
        NeighborInfo peer_ni = new NeighborInfo (req.peer, req.peer_guid);
        location_cache.add_node (peer_ni);

	NeighborInfo next_hop = calc_next_hop (req.dest, true);
	if (next_hop == my_neighbor_info) {
	    deliver (req.src, req.dest, req.peer, req.app_id, -1, //req.tries,
                     req.wait_ms, req.est_rtt_ms, req.payload);
	}
	else {
	    ApplicationInfo appInfo = apps.get(new Long(req.app_id));
	    if (req.intermediate_upcall && (appInfo != null) 
                    && (appInfo.routeUpcall != null)) {
                appInfo.routeUpcall.run(req.src, req.dest, req.peer, 
                                        new Long(req.wait_ms), 
                                        new Long(req.est_rtt_ms), req.payload);
	    }
	    else {
		if (req.intermediate_upcall && logger.isDebugEnabled ()) {
                    if (appInfo == null) {
                        logger.debug ("Could not upcall " + req +
                                ", no app.  Forwarding it towards the root.");
                    }
                    else {
                        logger.debug ("Could not upcall " + req +
                               ", no upcall.  Forwarding it towards the root.");
                    }
                }

		RouteMsg outb = new RouteMsg (next_hop.node_id,
			req.src, req.dest, req.app_id, req.intermediate_upcall,
			my_guid, req.payload);
                outb.comp_q = my_sink;
                outb.user_data = new RecursiveRouteCB (next_hop, 
                    new Runnable() { public void run() { handleEvent(req); }});
                if (no_rexmit_routes)
                    outb.timeout_sec = -1;
                else
                    outb.timeout_sec = 5;
		dispatch (outb);
	    }
	}
    }

    protected void set_initialized () {

	// Mimic Tapestry to make run scripts happy.
	System.out.println ("Tapestry: ready");

	initialized = true;
	try {
	    while (! waitq.isEmpty ())
		handleEvent (waitq.removeFirst ());
	}
	catch (Exception e) {
	    e.printStackTrace ();
	    BUG ("Caught " + e);
	}

        acore.registerTimer(randomPeriod(ls_alarm_period), leafSetAlarm);

        if (partition_check_alarm_period != 0) {
            long wait = 0;
            if (! immediate_join) {
                wait = randomPeriod(partition_check_alarm_period);
            }
            acore.registerTimer(wait, partitionCheckAlarm);
        }

        if (near_rt_alarm_period != 0) {
            acore.registerTimer(randomPeriod(near_rt_alarm_period), 
                                nearRoutingTableAlarm);
        }

        if (far_rt_alarm_period != 0) {
            acore.registerTimer(randomPeriod(far_rt_alarm_period),
                                farRoutingTableAlarm);
        }

        if (lookup_rt_alarm_period != 0) {
            DustDevil.acore_instance ().registerTimer(
                    randomPeriod(lookup_rt_alarm_period),
                    lookupRoutingTableAlarm);
        }
    }

    protected Runnable ready = new Runnable() {
        public void run() {
            network = Network.instance(my_node_id);
            vivaldi = Vivaldi.instance(my_node_id);
            rpc = Rpc.instance(my_node_id);
            try {
                rpc.registerRequestHandler(CoordReq.class, coordReqHandler);
            }
            catch (DuplicateTypeException e) { BUG(e); }
            start_time_ms = now_ms ();
            if (gateways.isEmpty ()) {
                logger.info ("Joined through gateway " + my_node_id);
                set_initialized ();
                notify_leaf_set_changed ();
            }
            else {
                NodeId gateway = gateways.removeFirst ();
                gateways.addLast (gateway);
                logger.info ("Trying to join through gateway " + gateway);
                dispatch (new JoinReq (gateway, my_node_id, my_guid, 0));
                acore.registerTimer(randomPeriod(10*1000), 
                        curry(joinAlarm, new Integer(0), 
                            new Integer(10*1000), // 10 seconds for starters
                            new Integer(0)));
            } 
            acore.registerTimer(randomPeriod(periodic_ping_period), pingAlarm);
            acore.registerTimer(randomPeriod(COORD_CHECK), expireCoordsAlarm);
            acore.registerTimer(randomPeriod(send_coord_period), 
                                sendCoordsAlarm);
        }
    };

    protected int weighted_random_rt_level () {
        // Prefer nodes in lower (closer) levels of the routing table.
        // Say there are nodes in three levels of our routing table.  Then we
        // will pick level 0 with probability 3/x, level 1 with probability
        // 2/x and level 2 with probability 1/x, where x=3+2+1.
        //
        // So, in the code below, choices[0] would be 3, choices[1] would be
        // 2, and choices[2] would be 1.  sum would be 6, and rval would be in
        // the range [1,6].  The rest should be obvious.
        //
        // NOTE: This code is a little sketchy, and I'm open to another way to
        // write it if anyone thinks one up.  It also might favor nodes in the
        // highest level a bit; although that level is unlikely to be picked,
        // there are likely very few nodes in it, giving them a greater net
        // probability of being picked than those nodes one level lower.

        if (rt.size () == 0)
            return 0;

        int highest_level = rt.highest_level ();
        int [] choices = new int [highest_level + 1];
        int sum = 0;
        for (int i = 0; i < choices.length; ++i) {
            sum += choices [i] = choices.length - i;
        }

        int rval = rand.nextInt (sum) + 1;
        int which = 0;
        while (true) {
            rval -= choices [which];
            if (rval <= 0)
                break;
            ++which;
        }
        return which;
    }

    protected Runnable nearRoutingTableAlarm = new Runnable() {
        public void run() {
            if (rt.size () > 0) {
                int which = weighted_random_rt_level ();
                int orig_which = which;

                if (logger.isDebugEnabled ())
                    logger.debug ("RT AE on level " + which);

                NeighborInfo ni = rt.random_neighbor (which, rand);

                // It's sometimes the case that (for example) we can have a
                // node on level 2 and no node on level 1.  In such cases, we
                // just give it to the next highest level.
                int highest_level = rt.highest_level ();
                while ((ni == null) && (which <= highest_level))
                    ni = rt.random_neighbor (++which, rand);

                if (ni == null)
                    BUG ("level=" + which + " highest=" + rt.highest_level ()
                            + " orig_level=" + orig_which + " rt=\n" + rt);

                // Try not to slam nodes that are already behind.
                if (! possibly_down.containsKey (ni)) {
                    RoutingTableReq outb =
                        new RoutingTableReq (ni.node_id, my_guid, which);
                    use_as_periodic_ping (ni, outb);
                    dispatch (outb);
                }
            }

            acore.registerTimer(randomPeriod(near_rt_alarm_period), this);
        }
    };

    protected static final int [] bit_select = {
	0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01
    };

    protected Runnable farRoutingTableAlarm = new Runnable() {
        public void run() {
            int digit = weighted_random_rt_level ();
            int value = rand.nextInt (DIGIT_VALUES);

            // Find someone else with the same digits as us in levels
            // which - 1 and below, and ask for their routing table at
            // that level.

            int [] lookup_digits = new int [GUID_DIGITS];
            for (int i = 0; i < GUID_DIGITS; ++i) {
                if (i < digit)
                    lookup_digits [i] = my_digits [i];
                else if (i == digit)
                    lookup_digits [i] = value;
                else
                    lookup_digits [i] = (byte) rand.nextInt (DIGIT_VALUES);
            }

            BigInteger lookup_guid = rt.digits_to_guid (lookup_digits);

            logger.debug ("looking for a closer neighbor at level "
                    + digit + " and value "
                    + Integer.toHexString (value) + " by looking up " +
                    GuidTools.guid_to_string (lookup_guid));

            BambooRouteInit outb = new BambooRouteInit (
                    lookup_guid, 0, false, false,
                    new LookupReqPayload (my_node_id));

            handleEvent (outb);

            acore.registerTimer(randomPeriod(far_rt_alarm_period), this);
        }
    };

    protected Runnable lookupRoutingTableAlarm = new Runnable() {
	public void run() {

            int highest = rt.highest_level ();
            if (logger.isDebugEnabled ()) logger.debug ("highest=" + highest);

            Set<int[]> choices = new LinkedHashSet<int[]> ();
            for (int digit = 0; digit <= highest; ++digit) {
                for (int value = 0; value < DIGIT_VALUES; ++value) {
                    NeighborInfo ni = rt.primary (digit, value);
                    if (ni == null)
                        choices.add (new int [] {digit, value});
                    if (logger.isDebugEnabled ())
                        logger.debug ("digit=" + digit + ", value="
                                + value + ", neighbor="
                                + (ni == null ? "null"
                                   : GuidTools.guid_to_string (ni.guid)));
                }
            }
            if (choices.size () != 0) {
                int which = rand.nextInt (choices.size ());
                int digit = 0, value = 0;
                {
                    Iterator<int[]> i = choices.iterator ();
                    while (which-- > 0)
                        i.next ();
                    int [] dv = i.next ();
                    digit = dv [0];
                    value = dv [1];
                }

                // Do a lookup on an identifier whose first digit digits are
                // the same as ours, and whose digit+1st value is value, and
                // whose remaining digits are random.

                int [] lookup_digits = new int [GUID_DIGITS];
                for (int i = 0; i < GUID_DIGITS; ++i) {
                    if (i < digit)
                        lookup_digits [i] = my_digits [i];
                    else if (i == digit)
                        lookup_digits [i] = value;
                    else
                        lookup_digits [i] = (byte) rand.nextInt (DIGIT_VALUES);
                }

                BigInteger lookup_guid = rt.digits_to_guid (lookup_digits);

                if (logger.isDebugEnabled ())
                    logger.debug ("trying to fill hole at digit="
                            + digit + " and value="
                            + Integer.toHexString (value) + " by looking up "
                            + GuidTools.guid_to_string (lookup_guid));

                BambooRouteInit outb = new BambooRouteInit (
                        lookup_guid, 0, false, false,
                        new LookupReqPayload (my_node_id));

                handleEvent (outb);
            }

            DustDevil.acore_instance ().registerTimer(
                    randomPeriod(lookup_rt_alarm_period), this);
        }
    };

    protected void handle_lookup_resp_msg (LookupRespMsg resp) {

        if (logger.isDebugEnabled ()) {
            logger.debug ("got lookup resp for 0x"
                    + GuidTools.guid_to_string (resp.lookup_id) + " from 0x"
                    + GuidTools.guid_to_string (resp.owner_id));
        }

	NeighborInfo ni = new NeighborInfo (resp.peer, resp.owner_id);
        add_to_rt (ni);
        location_cache.add_node (ni);

        PendingLookupInfo pl = pending_lookups.remove(resp.lookup_id);
        if (pl != null) {
            for (Pair<LookupCb,Object> pair : pl.cbs) {
                pair.first.lookup_cb(resp.lookup_id, resp.owner_id, 
                                     resp.peer, pair.second);
            }
        }
    }

    protected void handle_routing_table_req (RoutingTableReq req) {
	if (req.level >= GUID_DIGITS) {
            if (logger.isDebugEnabled ())
                logger.debug ("Got " + req + " with level too high.");
	    return;
	}

	NeighborInfo ni = new NeighborInfo (req.peer, req.guid);
        // add_to_rt (ni);
        location_cache.add_node (ni);

	LinkedList<NeighborInfo> neighbors = null;
	for (int value = 0; value < DIGIT_VALUES; ++value) {
	    if ((rt.primary (req.level, value) != null) &&
		    (rt.primary (req.level, value) != my_neighbor_info)) {
		if (neighbors == null) {
		    neighbors = new LinkedList<NeighborInfo>();
		}
		neighbors.addLast (rt.primary (req.level, value));
	    }
	}

	RoutingTableResp outb =
            new RoutingTableResp (req.peer, my_guid, neighbors);
        if (leaf_set.contains (ni) || rt.contains (ni)
                || reverse_rt.contains (ni))
            use_as_periodic_ping (ni, outb);
	dispatch (outb);
    }

    protected void handle_routing_table_resp (RoutingTableResp resp) {
        NeighborInfo peer_ni = new NeighborInfo (resp.peer, resp.peer_guid);
        location_cache.add_node (peer_ni);

        // Find all the nodes that are neither in our leaf set or routing
        // table from this response.

        LinkedList<NeighborInfo> new_nodes = null;
        if (resp.neighbors != null) {
            for (Iterator i = resp.neighbors.iterator (); i.hasNext (); ) {
                NeighborInfo ni = (NeighborInfo) i.next ();

                if (ni.guid.compareTo (MODULUS) >= 0) {
                    if (logger.isDebugEnabled ()) {
                        logger.debug ("got a guid with too many digits " +
                                "in a RoutingTableResp from " + resp.peer +
                                ".  Offending guid was " +
                                ni.guid.toString (16));
                        logger.debug ("msg was " + resp);
                    }
                    continue;
                }

                // Don't add nodes to the location cache w/o direct
                // confirmation that they're up, such as receiving a message
                // from them.
                // location_cache.add_node (ni);

                if (ni.node_id.equals (my_node_id)) {
                    if (logger.isDebugEnabled ()) logger.debug (ni + " is me.");
                }
                else if (leaf_set.contains (ni)) {
                    if (logger.isDebugEnabled ())
                        logger.debug (ni + " is in my LS.");
                }
                else if (rt.contains (ni)) {
                    if (logger.isDebugEnabled ())
                        logger.debug (ni + " is in my RT.");
                }
                else {
                    if (new_nodes == null) 
                        new_nodes = new LinkedList<NeighborInfo>();
                    new_nodes.addLast (ni);
                }
            }
        }

        if (new_nodes != null) {

            // Try and add any node which fills a hole.

            Iterator<NeighborInfo> i = new_nodes.iterator ();
            while (i.hasNext ()) {
                NeighborInfo ni = i.next ();
                if (rt.fills_hole (ni)) {
                    if (logger.isDebugEnabled ())
                        logger.debug (ni + " fills a hole.");
                    i.remove ();
                    add_to_rt (ni);
                }
                else if (logger.isDebugEnabled ())
                    logger.debug (ni + " doesn't fill a hole.");
            }

            // If there are any redundant nodes, pick one and see if it's
            // closer than the existing entry.

            if (new_nodes.size () > 0) {
                int which = rand.nextInt (new_nodes.size ());
                while (which-- > 0)
                    new_nodes.removeFirst ();

                NeighborInfo ni = new_nodes.getFirst ();
                add_to_rt (ni);

                // Also, if this node didn't send this response to me, I might
                // fill a hole in it's routing table.  I reuse the
                // RoutingTableResp message to say hello to it.
                //
                // NOTE: since I only send myself in this message, the
                // receiving node won't send one back to me by the following
                // check:

                if ((! pastry_mode) && (! ni.node_id.equals (resp.peer))) {
                    LinkedList<NeighborInfo> only_me = 
                        new LinkedList<NeighborInfo>();
                    only_me.addLast (my_neighbor_info);
                    RoutingTableResp outb =
                        new RoutingTableResp (ni.node_id, my_guid, only_me);
                    use_as_periodic_ping (ni, outb);
                    dispatch (outb);
                }
            }
        }
    }

    protected void notify_routing_table_changed (
	BambooNeighborInfo [] added, BambooNeighborInfo [] removed) {
        for (ApplicationInfo appInfo : apps.values()) {
            if (appInfo.routingTableChanged != null) 
                appInfo.routingTableChanged.run(added, removed);
        }
    }

    protected void notify_reverse_routing_table_changed (
	    BambooNeighborInfo [] added, BambooNeighborInfo [] removed) {
        for (ApplicationInfo appInfo : apps.values()) {
            if (appInfo.reverseRoutingTableChanged != null) 
                appInfo.reverseRoutingTableChanged.run(added, removed);
        }
    }

    protected void notify_leaf_set_changed () {
        notify_leaf_set_changed (null);
    }

    protected void notify_leaf_set_changed (ApplicationInfo onlyThisOne) {
	if (logger.isDebugEnabled ()) logger.debug ("notify_leaf_set_changed");

	LinkedList ls = leaf_set.as_list ();
	if ((ls.size () < leaf_set_size * 2) &&
	    (ls.size () > 0)) {

	    if ((! ls.getFirst ().equals (ls.getLast ())) ||
		(ls.size () % 2 != 0)) {

		// This is a temporary state of the leaf set.  Don't send it
		// up; instead wait for the final state.

		return;
	    }
	}
		
        BambooNeighborInfo preds[] = leaf_set.preds ();
        BambooNeighborInfo succs[] = leaf_set.succs ();

        if (onlyThisOne == null) {
            for (ApplicationInfo appInfo : apps.values()) {
                if (appInfo.leafSetChanged != null) 
                    appInfo.leafSetChanged.run(preds, succs);
            }
        }
        else if (onlyThisOne.leafSetChanged != null) {
            onlyThisOne.leafSetChanged.run(preds, succs);
        }
    }

    protected Thunk3<Integer,Integer,Integer> joinAlarm = 
            new Thunk3<Integer,Integer,Integer>() {
        public void run(Integer tries, Integer period, Integer revTTL) {
            if (! initialized) {
                tries = new Integer(tries.intValue() + 1);
                revTTL = new Integer(revTTL.intValue() + 1);
                period = new Integer(period.intValue() >= 30*1000 
                                     ? 60*1000 : period.intValue() * 2);
                int divisor = Math.max (3, gateways.size ());
                NodeId gateway = gateways.removeFirst ();
                gateways.addLast (gateway);
                logger.info ("Join try " + tries +
                        " timed out.  Gateway=" + gateway + ".  Trying again " +
                        " with rev_ttl=" + revTTL.intValue()/divisor);
                dispatch (new JoinReq (gateway, my_node_id, my_guid, 
                                       revTTL.intValue()/divisor));
                acore.registerTimer(randomPeriod(period.intValue()),
                                    curry(this, tries, period, revTTL));
            }
        }
    };

    protected void handle_join_req (JoinReq req) {

        // Check for routing loops, and if one is found, just drop the
        // message.  Either the network will heal, or the rev_ttl will be
        // increased by the joining node until the loop is avoided.

        if (req.path.contains (my_neighbor_info)) {
            logger.warn ("loop in join path: " + req);
            return;
        }

        NeighborInfo joiner = new NeighborInfo (req.node_id, req.guid);

        // Don't use location cache for joins.
	NeighborInfo next_hop = calc_next_hop (req.guid, false);
        int hops_to_go = est_hops_to_go (req.guid, false);

        // Don't add nodes to the location cache w/o direct
        // confirmation that they're up, such as receiving a message
        // from them.
        // location_cache.add_node (joiner);

	if ((hops_to_go == req.rev_ttl) ||
            (next_hop == my_neighbor_info) ||
	    next_hop.node_id.equals (req.node_id)) {

	    if (next_hop.node_id.equals (req.node_id)
                && logger.isDebugEnabled ())
                logger.debug ("next hop for " + req + " is joining node!");

	    // We're the root.  Send a response.

	    LinkedList<NeighborInfo> path = new LinkedList<NeighborInfo>();
            for (NeighborInfo n : req.path) 
                path.addLast(n);
	    path.addLast (my_neighbor_info);

	    dispatch (new JoinResp (req.node_id, path, leaf_set.as_set ()));

	    // Add it to our leaf set and routing table.

	    add_to_rt (joiner);
	    add_to_ls (joiner);
	}
	else {
            final JoinReq orig = req;
	    try {
		req = (JoinReq) req.clone ();
	    }
	    catch (CloneNotSupportedException e) {
                BUG (e);
	    }
	    req.path.addLast (my_neighbor_info);
	    req.peer = next_hop.node_id;
	    req.inbound = false;
            req.comp_q = my_sink;
            req.user_data = new RecursiveRouteCB (next_hop, 
                    new Runnable() { public void run() { handleEvent(orig); }});
            req.timeout_sec = 5;
	    dispatch (req);
	}
    }

    protected void handle_join_resp (JoinResp resp) {

	// The one who sent this message should be last in the path.

	NeighborInfo my_root = (NeighborInfo) resp.path.getLast ();
	if (! my_root.node_id.equals (resp.peer)) {
	    logger.warn ("my_root=" + my_root + ", but join resp.peer=" +
		    resp.peer + ".  Retrying join.");
	    // Wait for timeout.
	    return;
	}

	// Add the node that sent the response...

	add_to_rt (my_root);
	add_to_ls (my_root);
        location_cache.add_node (my_root);

	// ...and all the people in its leaf set.  Note that if we're already
        // initialized, this is a JoinResp in response to our looking for
        // evidence of a partition.  If there hasn't been one, a down node has
        // come back up and routed our existing leaf set back to us, in which
        // case the add_to_ls calls will have no effect.  On the other hand,
        // the add_to_rt calls might, and we have a separate mechanism to
        // handle optimizing the RT, so we don't do anything to the RT here.

	for (Iterator i = resp.leaf_set.iterator (); i.hasNext (); ) {
	    NeighborInfo ni = (NeighborInfo) i.next ();
	    if (! ni.node_id.equals (my_node_id)) {
		add_to_ls (ni);
                if (! initialized)
                    add_to_rt (ni);
                // Don't add nodes to the location cache w/o direct
                // confirmation that they're up, such as receiving a message
                // from them.
                // location_cache.add_node (ni);
	    }
	}

	// At this point, we can route queries.
        //
        // If none of these pings come back, we will think we're the root for
        // everything.  It should get handled by the partition healing code
        // eventually, though.

        if (! initialized) {
	    logger.info ("Joined through gateway " + resp.path.getFirst ());

            set_initialized ();

            // Add the nodes from the join path to the routing table.  (The
            // maintenance algorithm will fill in the rest.)

            for (Iterator i = resp.path.iterator (); i.hasNext (); ) {
                NeighborInfo ni = (NeighborInfo) i.next ();
                add_to_rt (ni);
                // Don't add nodes to the location cache w/o direct
                // confirmation that they're up, such as receiving a message
                // from them.
                // location_cache.add_node (ni);
            }
        }
    }

    //////////////////////////////////////////////////////////////////////
    //
    //  		      Probe Ping functions
    //
    //////////////////////////////////////////////////////////////////////

    protected Set<NeighborInfo> pings_in_flight = 
        new LinkedHashSet<NeighborInfo>();

    protected void send_ping (NeighborInfo ni) {
	if (! pings_in_flight.contains (ni)) {

	    pings_in_flight.add (ni);

            PingMsg outb = new PingMsg (ni.node_id);
            outb.comp_q = my_sink;
            outb.user_data = new ProbePingCB (ni);
            outb.timeout_sec = 5;
            dispatch (outb);
	}
    }

    protected class ProbePingCB implements NetMsgResultCB {
        NeighborInfo ni;
        public ProbePingCB (NeighborInfo n) { ni = n; }
        public void success () {
            pings_in_flight.remove (ni);
            generic_msg_success (ni);
            dispatch (new NetworkLatencyReq (ni.node_id, my_sink, ni));
        }
        public void failure () {
	    pings_in_flight.remove (ni);
        }
        public String toString () { return "(ProbePingCB " + ni + ")"; }
    }

    protected void handle_net_lat_resp (NetworkLatencyResp resp) {

        if (! resp.success) return;

        NeighborInfo ni = (NeighborInfo) resp.user_data;

        // We got this resp. b/c we think the node is up.  But if it's in
        // possibly_down, we'll never ping it again.  So pretend for the
        // moment that its up for sure.

        generic_msg_success (ni);

        // Put this in here so that the add_to_.* functions can see it...
        Double rtt_ms = new Double ((double) resp.rtt_ms);
        if (rtt_ms == null)
            BUG ("compiler bug");
        latency_map.put (ni, rtt_ms);

	add_to_ls_ping_time (ni, resp.rtt_ms);
	add_to_rt_ping_time (ni, resp.rtt_ms);
        location_cache.add_node (ni);

        // ... but don't store latencies for nodes we're not actually
        // monitoring.
        if ((! leaf_set.contains (ni)) && (! rt.contains (ni))
                && (! reverse_rt.contains (ni))) {
            latency_map.remove (ni);
        }
    }

    //////////////////////////////////////////////////////////////////////
    //
    //  		       Leaf set functions
    //
    //////////////////////////////////////////////////////////////////////

    protected void add_to_ls (NeighborInfo ni) {
        if (! leaf_set.contains (ni)) {
            if (have_rtt_ms (ni))
                add_to_ls_ping_time (ni, Double.MAX_VALUE);
            else
                send_ping (ni);
        }
    }

    protected boolean add_to_ls_ping_time (NeighborInfo ni, double rtt_ms) {
	NeighborInfo r = leaf_set.add_node (ni);

	if (r != null) {

            if (logger.isInfoEnabled ()) {

                StringBuffer buf = null;

                if (r == my_neighbor_info) {
                    buf = new StringBuffer (50);
                    buf.append ("added ");
                    buf.append (ni.node_id.address ().getHostAddress ());
                    buf.append (":");
                    buf.append (ni.node_id.port ());
                    buf.append (" to leaf set");
                }
                else {
                    buf = new StringBuffer (80);
                    buf.append ("replaced ");
                    buf.append (r.node_id.address ().getHostAddress ());
                    buf.append (":");
                    buf.append (r.node_id.port ());
                    buf.append (" with ");
                    buf.append (ni.node_id.address ().getHostAddress ());
                    buf.append (":");
                    buf.append (ni.node_id.port ());
                    buf.append (" in leaf set");
                }

                logger.info (buf);
            }

	    // Notify local node.

	    notify_leaf_set_changed ();

	    return true;
	}
	return false;
    }

    protected boolean remove_from_ls (NeighborInfo ni) {
	int result = leaf_set.remove_node (ni);
	if (result != LeafSet.REMOVED_NONE) {

            if (logger.isInfoEnabled ()) {
                StringBuffer buf = new StringBuffer (50);
                buf.append ("removed ");
                buf.append (ni.node_id.address ().getHostAddress ());
                buf.append (":");
                buf.append (ni.node_id.port ());
                buf.append (" from leaf set");
                logger.info (buf);
            }

	    // It may be the case that there is no one left in our leaf
	    // set.  If so, try and recover it using the routing table.

	    boolean more_added = false;
	    if (leaf_set.as_set ().isEmpty ()) {
		for (int level = 0; level < GUID_DIGITS; ++level) {
		    for (int digit = 0; digit < DIGIT_VALUES; ++digit) {
			NeighborInfo primary = rt.primary (level, digit);
			if ((primary != null) &&
			    (primary != my_neighbor_info) &&
			    (! primary.equals (ni))) {

			    more_added = add_to_ls_ping_time (primary,
				    rtt_ms (primary)) || more_added;
			}
		    }
		}
	    }

	    // If other nodes were added to the leaf set, applications will
	    // have been notified because of that, so we don't need to do
	    // it again.

	    if (! more_added)
		notify_leaf_set_changed ();

	    return true;
	}

	return false;
    }

    //////////////////////////////////////////////////////////////////////
    //
    //  		  Routing table functions
    //
    //////////////////////////////////////////////////////////////////////

    protected void add_to_rt (NeighborInfo ni) {
        if (! rt.contains (ni)) {
            if (have_rtt_ms (ni))
                add_to_rt_ping_time (ni, rtt_ms (ni));
            else
                send_ping (ni);
        }
    }

    protected boolean add_to_rt_ping_time (NeighborInfo ni, double rtt_ms) {

	NeighborInfo r = rt.add (ni, rtt_ms, PNS, now_ms ());
	if (r == null)
            return false;

        BambooNeighborInfo [] removed = null;
        if (r != my_neighbor_info) {
            removed = new BambooNeighborInfo [1];
            removed [0] =
                new BambooNeighborInfo (r.node_id, r.guid, rtt_ms (r));

            dispatch (new RoutingNeighborAnnounce (
                        r.node_id, my_guid, false));
        }

        dispatch (new RoutingNeighborAnnounce (ni.node_id, my_guid, true));

        BambooNeighborInfo [] added = {
            new BambooNeighborInfo (ni.node_id, ni.guid, rtt_ms)
        };
        notify_routing_table_changed (added, removed);

        if (logger.isInfoEnabled ()) {
            StringBuffer buf = null;
            if (r == my_neighbor_info) {
                buf = new StringBuffer (50);
                buf.append ("added ");
                buf.append (ni.node_id.address ().getHostAddress ());
                buf.append (":");
                buf.append (ni.node_id.port ());
                buf.append (" to routing table");
            }
            else {
                buf = new StringBuffer (80);
                buf.append ("replaced ");
                buf.append (r.node_id.address ().getHostAddress ());
                buf.append (":");
                buf.append (r.node_id.port ());
                buf.append (" with ");
                buf.append (ni.node_id.address ().getHostAddress ());
                buf.append (":");
                buf.append (ni.node_id.port ());
                buf.append (" in routing table");
            }
            logger.info (buf);
        }

        return true;
    }

    protected boolean remove_from_rt (NeighborInfo ni) {

	int level = rt.remove (ni);
	if (level == -1)
	    return false;

        if (logger.isInfoEnabled ()) {
            StringBuffer buf = new StringBuffer (50);
            buf.append ("removed ");
            buf.append (ni.node_id.address ().getHostAddress ());
            buf.append (":");
            buf.append (ni.node_id.port ());
            buf.append (" from routing table");
            logger.info (buf);
        }

	BambooNeighborInfo [] removed = {
	    new BambooNeighborInfo (ni.node_id, ni.guid)
	};

	notify_routing_table_changed (null, removed);

	return true;
    }

    //////////////////////////////////////////////////////////////////////
    //
    // 		       Reverse Routing Table Functions
    //
    //////////////////////////////////////////////////////////////////////

    protected void add_to_rrt (NeighborInfo ni) {
	if (! reverse_rt.contains (ni)) {

	    reverse_rt.add (ni);

	    BambooNeighborInfo [] added = {
		new BambooNeighborInfo (ni.node_id, ni.guid)
	    };

	    notify_reverse_routing_table_changed (added, null);
	}
    }

    protected boolean remove_from_rrt (NeighborInfo ni) {
        if (reverse_rt.remove (ni)) {
            BambooNeighborInfo [] removed = {
                new BambooNeighborInfo (ni.node_id, ni.guid)
            };

            notify_reverse_routing_table_changed (null, removed);
            return true;
        }

        return false;
    }

    //////////////////////////////////////////////////////////////////////
    //
    // 		          Node monitoring functions
    //
    //////////////////////////////////////////////////////////////////////

    protected boolean have_rtt_ms (NeighborInfo ni) {
	return latency_map.containsKey (ni);
    }

    protected double rtt_ms (NeighborInfo ni) {
	Double result = latency_map.get (ni);
        if (result == null) {
            logger.fatal ("No latency for " + ni);
            Iterator i = latency_map.keySet ().iterator ();
            while (i.hasNext ()) {
                NeighborInfo other = (NeighborInfo) i.next ();
                Double lat = latency_map.get (other);
                logger.fatal ("  " + other + ", " + lat + " ms");
            }
            throw new IllegalStateException ("No latency for " + ni);
        }
	return result.doubleValue ();
    }

    //////////////////////////////////////////////////////////////////////
    //
    // 		              Routing functions
    //
    //////////////////////////////////////////////////////////////////////

    /**
     * Calculate the next hop using the standard algorithm.  If we're within
     * the range of the leaf set, returns the root.  Otherwise, if we can
     * resolve another digit using a neighbor in the routing table, returns
     * that neighbor.  If neither of the above, returns the closest node in
     * the leaf set.  If my_neighbor_info is returned, we're the root.
     */
    public NeighborInfo calcNextHop(BigInteger key) {
        return calc_next_hop(key, false);
    }

    protected NeighborInfo calc_next_hop (BigInteger guid, boolean use_lc) {

	// It's important to check the leaf set first.  Consider:
	//
	//   search guid:  0x30000000
	//   my guid:      0x2fffffff
	//   another node: 0x3fffffff
	//
	// Clearly, I'm closer than the other node, but if I were to make
	// my forwarding decision using the routing table, it would tell me
	// to go to the other node, since it has a longer matching prefix
	// than me with the search guid.  Since the end goal is to find the
	// node with the closest (numerically) guid to the search guid,
	// then, we always check the leaf set first.

        Set<NeighborInfo> ignore = ignore_possibly_down
            ? (new LinkedHashSet<NeighborInfo>()) : possibly_down.keySet();

	if (leaf_set.within_leaf_set (guid)) {
	    NeighborInfo result = leaf_set.closest_leaf (guid, ignore);
	    if (logger.isDebugEnabled ()) logger.debug (
		    "dst is in our leaf set, closest to " + result);
	    return result;
	}
	else {

	    NeighborInfo next_hop = rt.next_hop (guid, ignore);
            NeighborInfo lc_nh =
                use_lc ? location_cache.closest_node (guid) : null;

	    if (next_hop == null) {
		next_hop = leaf_set.closest_leaf (guid, ignore);
                if (lc_nh != null) {
                    BigInteger nhd = calc_dist (next_hop.guid, guid);
                    BigInteger lc_nhd = calc_dist (lc_nh.guid, guid);
                    if (lc_nhd.compareTo (nhd) < 0) {
			if (logger.isDebugEnabled ()) logger.debug (
                                "dst not in leaf set, and matches hole in " +
                                " routing table; using " + lc_nh +
                                " from location cache");
                        return lc_nh;
                    }
                }

		if (logger.isDebugEnabled ()) logger.debug (
			"dst not in leaf set, and matches hole in routing" +
			" table; using " + next_hop + " from leaf set");
		return next_hop;
	    }
	    else {
                if (lc_nh != null) {
                    BigInteger nhd = calc_dist (next_hop.guid, guid);
                    BigInteger lc_nhd = calc_dist (lc_nh.guid, guid);
                    if (lc_nhd.compareTo (nhd) < 0) {
                        if (logger.isDebugEnabled ()) logger.debug (
                                "dst not in leaf set, using " + lc_nh +
                                " from location cache");
                        return lc_nh;
                    }
                }

		if (logger.isDebugEnabled ()) logger.debug (
			"dst not in leaf set, using " + next_hop +
			" from routing table");
		return next_hop;
	    }
	}
    }

    public LinkedHashMap<NeighborInfo,Long> allNeighbors() {
        Set<NeighborInfo> n = leaf_set.as_set();
        n.addAll(rt.as_list());
        LinkedHashMap<NeighborInfo,Long> m = 
            new LinkedHashMap<NeighborInfo,Long>();
        for (NeighborInfo ni : n) {
            long lat = network.estimatedRTTMillis(ni.node_id);
            m.put(ni, new Long(lat == -1 ? Long.MAX_VALUE : lat));
        }
        return m;
    }

    /**
     * Calculates the next hop using Proximity Route Selection (PRS).  I.e.
     * make monotonic progress towards the root, but otherwise go to the
     * neighbor closest in network latency.  If my_neighbor_info is returned,
     * we're the root.
     */
    public NeighborInfo calcNextHopPRS(BigInteger key) {
        return calcNextHopPRS(key, allNeighbors());
    }

    /**
     * Calculates the next hop using Proximity Route Selection (PRS).  I.e.
     * make monotonic progress towards the root, but otherwise go to the
     * neighbor closest in network latency.  If my_neighbor_info is returned,
     * we're the root.
     */
    public NeighborInfo calcNextHopPRS(BigInteger key, 
                                       Map<NeighborInfo,Long> allNeighbors) {

        NeighborInfo result = null;
        BigInteger current = calc_dist(my_guid, key);
        long min = Long.MAX_VALUE;
        for (NeighborInfo ni : allNeighbors.keySet()) {
            if (!possibly_down.containsKey(ni)) {
                // This neighbor seems to be up.

                BigInteger dist = calc_dist(ni.guid, key);
                if ((dist.compareTo(current) < 0) 
                    || ((dist.compareTo(current) == 0) 
                        && in_range_mod (my_guid, ni.guid, key) 
                        && (!in_range_mod (ni.guid, my_guid, key)))) {
                    // And they make monotonic progress in the key space.

                    long lat = allNeighbors.get(ni).longValue();
                    if (result == null || lat < min) {
                        // And they're the closest such node in the
                        // physical network space.
                        result = ni;
                        min = lat;
                    }
                }
            }
        }
        if (result == null) 
            result = my_neighbor_info;
        return result;
    }

    /**
     * Calculates the next hop using scaled Proximity Route Selection (PRS).
     * I.e.  make monotonic progress towards the root, but otherwise go to the
     * neighbor that makes the most progress / network latency.  If
     * my_neighbor_info is returned, we're the root.
     */
    public NeighborInfo calcNextHopScaledPRS(BigInteger key, 
            Function2<BigInteger,BigInteger,Long> scalingFunc) {
        return calcNextHopScaledPRS(key, scalingFunc, allNeighbors());
    }

    /**
     * Calculates the next hop using scaled Proximity Route Selection (PRS).
     * I.e.  make monotonic progress towards the root, but otherwise go to the
     * neighbor that makes the most progress / network latency.  If
     * my_neighbor_info is returned, we're the root.
     */
    public NeighborInfo calcNextHopScaledPRS(BigInteger key, 
            Function2<BigInteger,BigInteger,Long> scalingFunc,
            Map<NeighborInfo,Long> allNeighbors) {

        NeighborInfo result = null;
        BigInteger current = calc_dist(my_guid, key);
        BigInteger max = BigInteger.ZERO;
        for (NeighborInfo ni : allNeighbors.keySet()) {
            if (!possibly_down.containsKey(ni)) {
                // This neighbor seems to be up.

                BigInteger dist = current.subtract(calc_dist(ni.guid, key));
                if ((dist.compareTo(BigInteger.ZERO) > 0) 
                    || ((dist.compareTo(BigInteger.ZERO) == 0) 
                        && in_range_mod (my_guid, ni.guid, key) 
                        && (!in_range_mod (ni.guid, my_guid, key)))) {
                    // And they make monotonic progress in the key space.

                    long lat = allNeighbors.get(ni).longValue();
                    BigInteger scaled = scalingFunc.run(dist, new Long(lat));
                    if (result == null || scaled.compareTo(max) > 0) {
                        // And they make the most progress / latency.
                        result = ni;
                        max = scaled;
                    }
                }
            }
        }
        if (result == null) 
            result = my_neighbor_info;
        return result;
    }

    public static Function2<BigInteger,BigInteger,Long> greedyScaling =
        new Function2<BigInteger,BigInteger,Long>() {
            public BigInteger run(BigInteger dist, Long lat) { return dist; }
        };

    /**
     * Calculates the next hop using greedy routing.
     */
    public NeighborInfo calcNextHopGreedy(BigInteger key, 
                                          Map<NeighborInfo,Long> allNeighbors) {
        return calcNextHopScaledPRS(key, greedyScaling, allNeighbors);
    }

    /**
     * Calculates the next hop using greedy routing.
     */
    public NeighborInfo calcNextHopGreedy(BigInteger key) {
        return calcNextHopScaledPRS(key, allNeighbors());
    }

    public static Function2<BigInteger,BigInteger,Long> prsScaling =
        new Function2<BigInteger,BigInteger,Long>() {
            public BigInteger run(BigInteger dist, Long lat) {
                return dist.divide(BigInteger.valueOf(lat.longValue()));
            }
        };

    public NeighborInfo calcNextHopScaledPRS(BigInteger key) {
        return calcNextHopScaledPRS(key, allNeighbors());
    }

    public NeighborInfo calcNextHopScaledPRS(BigInteger key,
            Map<NeighborInfo,Long> allNeighbors) {
        return calcNextHopScaledPRS(key, prsScaling, allNeighbors);
    }

    protected int est_hops_to_go (BigInteger guid, boolean use_lc) {
        Set<NeighborInfo> ignore = possibly_down.keySet();
	if (leaf_set.within_leaf_set (guid)) {
            if (leaf_set.closest_leaf (guid, ignore) == my_neighbor_info)
                return 0;
            else
                return 1;
	}
	else {
            return rt.highest_level () - rt.matching_digits (guid);
        }
    }

    public BigInteger calc_dist (BigInteger a, BigInteger b) {
        return GuidTools.calc_dist (a, b, MODULUS);
    }

    public boolean in_range_mod(
	    BigInteger low, BigInteger high, BigInteger query) {
	return GuidTools.in_range_mod(low, high, query, MODULUS);
    }

    protected void add_to_down_nodes (NodeId n) {
        if (down_nodes_cap > 0) {
            down_nodes.remove (n);
            down_nodes.add (n);
            if (down_nodes.size () > down_nodes_cap) {
                Iterator<NodeId> i = down_nodes.iterator ();
                assert i.hasNext ();
                i.next ();
                i.remove ();
            }
        }
    }

    public interface LookupCb {
        void lookup_cb (BigInteger lookup_id, BigInteger closest_id,
                        NodeId closest_addr, Object user_data);
    }

    protected Map<BigInteger,PendingLookupInfo> pending_lookups = 
        new LinkedHashMap<BigInteger,PendingLookupInfo>();

    protected static class PendingLookupInfo {
        public LinkedList<Pair<LookupCb,Object>> cbs = 
            new LinkedList<Pair<LookupCb,Object>>();
        public long last_start_time;
        public PendingLookupInfo (long l) { last_start_time = l; }
    }

    public void lookup (BigInteger id, LookupCb cb, Object user_data) {
        PendingLookupInfo pl = pending_lookups.get(id);
        if (pl == null) {
            pl = new PendingLookupInfo (now_ms ());
            pending_lookups.put (id, pl);
            BambooRouteInit outb = new BambooRouteInit (
                    id, 0, false, false, new LookupReqPayload (my_node_id));
            classifier.dispatch_later (outb, 0);
            acore.registerTimer(60*1000, curry(lookupTimeout, id));
        }
        pl.cbs.addLast (new Pair<LookupCb,Object>(cb, user_data));
    }

    protected Thunk1<BigInteger> lookupTimeout = new Thunk1<BigInteger>() {
        public void run(BigInteger id) {
            PendingLookupInfo pl = pending_lookups.get(id);
            if ((pl != null) && (now_ms () > 60*1000 + pl.last_start_time)) {
                pl.last_start_time = now_ms ();
                BambooRouteInit outb = new BambooRouteInit (
                        id, 0, false, false, new LookupReqPayload (my_node_id));
                classifier.dispatch_later (outb, 0);
                acore.registerTimer(60*1000, curry(this, id));
            }
        }
    };

    ///////////////////////////////////////////////////////////////////    
    //
    //      Code to keep track of neighbors Vivaldi coordinates.
    //
    ///////////////////////////////////////////////////////////////////    

    public VirtualCoordinate coordinate(NeighborInfo ni) {
        Pair<VirtualCoordinate,Long> p = coords.get(ni);
        return (p == null) ? null : p.first;
    }

    protected Map<NeighborInfo,Pair<VirtualCoordinate,Long>> coords = 
        new LinkedHashMap<NeighborInfo,Pair<VirtualCoordinate,Long>>();

    protected static int COORD_CHECK = 60*1000;

    protected Runnable expireCoordsAlarm = new Runnable() {
        public void run() {
            long timer_ms = timer_ms();
            Iterator<NeighborInfo> i = coords.keySet().iterator();
            while (i.hasNext()) {
                NeighborInfo ni = i.next();
                Pair<VirtualCoordinate,Long> p = coords.get(ni);
                if (p.second.longValue() + COORD_CHECK > timer_ms)
                    i.remove();
            }
            acore.registerTimer(randomPeriod(COORD_CHECK), this);
        }
    };

    protected LinkedList<NeighborInfo> sendCoordsShuffle = 
        new LinkedList<NeighborInfo>();

    protected int send_coord_period = 1000;

    protected Runnable sendCoordsAlarm = new Runnable() {
        public void run() {
            if (sendCoordsShuffle.isEmpty()) {
                Set<NeighborInfo> s = leaf_set.as_set();
                s.addAll(rt.as_list());
                sendCoordsShuffle.addAll(s);
            }
            if (!sendCoordsShuffle.isEmpty()) {
                NeighborInfo ni = sendCoordsShuffle.removeFirst();
                CoordReq req = 
                    new CoordReq(my_guid, vivaldi.localCoordinates()); 
                rpc.sendRequest(ni.node_id, req, 5, CoordResp.class, 
                                curry(coordRespHandler, ni), null);
            }
            acore.registerTimer(randomPeriod(send_coord_period), this);
        }
    };

    protected Thunk2<NeighborInfo,CoordResp> coordRespHandler = 
        new Thunk2<NeighborInfo,CoordResp>() {
        public void run(NeighborInfo ni, CoordResp resp) {
            coords.put(ni, Pair.create(resp.coords, new Long(timer_ms())));
        }
    };

    protected Thunk3<InetSocketAddress,CoordReq,Object> coordReqHandler = 
        new Thunk3<InetSocketAddress,CoordReq,Object>() {
        public void run(InetSocketAddress peer, CoordReq req, Object respTok) {
            NeighborInfo other = 
                new NeighborInfo(NodeId.create(peer), req.srcID);
            coords.put(other, Pair.create(req.srcCoords, new Long(timer_ms())));
            VirtualCoordinate local = vivaldi.localCoordinates();
            rpc.sendResponse(new CoordResp(vivaldi.localCoordinates()),respTok);
        }
    };
}

