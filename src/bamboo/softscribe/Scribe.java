/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.softscribe;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.HashMap;
import java.util.Vector;
import java.util.Enumeration;
import ostore.dispatch.Classifier;
import ostore.util.ByteArrayInputBuffer;
import ostore.util.ByteArrayOutputBuffer;
import ostore.util.Carp;
import ostore.util.CountBuffer;
import ostore.util.Debug;
import ostore.util.DebugFlags;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.Pair;
import ostore.util.QSException;
import ostore.util.QSLong;
import ostore.util.QuickSerializable;
import ostore.util.SecureHash;
import ostore.util.SHA1Hash;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.EventHandlerIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import seda.sandStorm.api.SinkException;
import bamboo.api.BambooLeafSetChanged;
import bamboo.api.BambooNeighborInfo;
import bamboo.api.BambooRouteContinue;
import bamboo.api.BambooRouteDeliver;
import bamboo.api.BambooRouteInit;
import bamboo.api.BambooRouterAppRegReq;
import bamboo.api.BambooRouterAppRegResp;
import bamboo.api.BambooRoutingTableChanged;
import bamboo.api.BambooReverseRoutingTableChanged;
import bamboo.api.BambooRouteUpcall;
import bamboo.dmgr.PutOrRemoveReq;
import bamboo.dmgr.PutOrRemoveResp;
import bamboo.db.StorageManager;
import bamboo.lss.NioInputBuffer;
import bamboo.util.GuidTools;
import ostore.util.QSString;

/**
 * Scribe on Bamboo.
 *
 * @author David Oppenheimer
 * @version $Id: Scribe.java,v 1.3 2005/08/21 22:58:40 srhea Exp $
 *
 * Unlike Scribe, there is no CREATE operation. Also unlike Scribe, there are no
 *  explicit JOIN and LEAVE network messages; those are purely local operations.
 *
 * allChildGroups is a hashtable mapping a groupguid (the ID for a group, which
 *  is the destination GUID for messages for that group, and hence the
 *  determinant of the root of the multicast tree for that groupguid) to a
 *  hashtable. That second hashtable maps NodeID's of children of this node to
 *  Booleans indicating whether we have heard from that child since the last
 *  ChildAlarm was received. (We could store a timestamp instead.)
 *  myLocalGroups is a vector of groupguids of groups to which this node am
 *  subscribed.
 *
 * When a node wants to join a group, and periodically after it has joined, it
 *  routes an MCastJoinMsg to the groupguid. Upcalls at non-member nodes simply
 *  continue. When the message upcalls at the first member node (or the root),
 *  that node records the original sender in allChildGroups, marks the node as
 *  being heard from, and kills the message.
 *
 * When the ChildAlarm expires, we delete from allChildGroups all children from
 *  which we have not heard since the last ChildAlarm expired (and reset the
 *  heard-from flag).
 * 
 * When a node multicasts, it routes an MCastUpMsg to the groupguid. Along the
 *  path, the message is forwarded. When the message reaches the root, the root
 *  generates a MCastDownMsg *network* (not Bamboo) message and sends it to each
 *  of its children for that tree. At each node, if the node is in the group, a
 *  local MCastDown is dispatched. In all cases the message is re-sent to that
 *  node's children, and so on.
 *
 * */
public class Scribe extends ostore.util.StandardStage 
implements SingleThreadedEventHandlerIF {
    protected boolean DEBUG_MIN = false;
    
    protected Random rand;
    protected boolean initialized;
    protected LinkedList wait_q = new LinkedList ();
    protected static final long app_id = ostore.util.ByteUtils.bytesToLong (
	    (new SHA1Hash ("bamboo.softscribe.Scribe")).bytes (), new int [1]);
    protected static class ParentAlarm implements QueueElementIF {}
    protected static class ChildAlarm implements QueueElementIF {}
    protected BigInteger my_guid;
    protected SecureHash my_guid_sh;
    // heartbeat to our parent every 10 seconds
    private static int PARENT_ALARM_INTERVAL = 10000; 
    private static int PARENT_ALARM_VARIATION = 3000;
    // give up on a child after 30 seconds
    private static int CHILD_ALARM_INTERVAL = 30000;
    private static int CHILD_ALARM_VARIATION = 3000;
    
    // map groupguid to a map of children in that group and whether I've heard
    // from them recently
    // BigInteger groupguid -> Map of (NodeId's -> Boolean) 
    public HashMap allChildGroups = new HashMap();
    // a vector of groups (BigInteger groupguid) that I am a member of
    public Vector myLocalGroups = new Vector();

    public Scribe () throws Exception {
	DEBUG = true;

	// bamboo payloads
	ostore.util.TypeTable.register_type ("bamboo.softscribe.MCastUpMsg");
	ostore.util.TypeTable.register_type ("bamboo.softscribe.MCastJoinMsg");
	// goes to client stage
	ostore.util.TypeTable.register_type ("bamboo.softscribe.MCastDown");

	String [] event_types = {
	    "seda.sandStorm.api.StagesInitializedSignal",
	    "bamboo.softscribe.Scribe$ParentAlarm",
	    "bamboo.softscribe.Scribe$ChildAlarm"
	};
	this.event_types = event_types;

	// These events are sent to us from the network
	String [] inb_msg_types = { 
	    "bamboo.softscribe.MCastDownMsg"
	};
	this.inb_msg_types = inb_msg_types; 

	// Sent to us from a higher-level stage, going towards the wire
	String [] outb_msg_types = {
	    "bamboo.softscribe.MCastUp",
	    "bamboo.softscribe.MCastJoin"
	};
	this.outb_msg_types = outb_msg_types;
    }

    public void init (ConfigDataIF config) throws Exception {
	super.init (config);
	int debug_level = config.getInt ("debug_level");
	if (debug_level > 0) {
	    DEBUG = true;
	    Debug.printtagln(tag, "DDDDD Scribe DEBUG true");
	}
    }

    public void handleEvent (QueueElementIF item) {
	if (DEBUG) Debug.printtagln (tag, "handleEvent got " + item);

	if (item instanceof seda.sandStorm.api.StagesInitializedSignal) {
	    dispatch (new BambooRouterAppRegReq (
			app_id, false, false, false, my_sink));
	}
	else if (item instanceof BambooRouterAppRegResp) { 
	    BambooRouterAppRegResp resp = (BambooRouterAppRegResp) item;

            my_guid = resp.node_guid;
	    my_guid_sh = GuidTools.big_integer_to_secure_hash (my_guid);
	    {
	    byte [] noise = my_guid_sh.bytes ();
	    int seed = ostore.util.ByteUtils.bytesToInt (noise, new int [1]);
	    rand = new Random(seed);
	    }
	    initialized = true;
	    while (! wait_q.isEmpty ()) 
		handleEvent ((QueueElementIF) wait_q.removeFirst ());
	    classifier.dispatch_later(new ParentAlarm(), PARENT_ALARM_INTERVAL + 
				       rand.nextInt (PARENT_ALARM_VARIATION));
	    classifier.dispatch_later(new ChildAlarm(),  CHILD_ALARM_INTERVAL + 
				       rand.nextInt(CHILD_ALARM_VARIATION));
	}
	else if (! initialized) {
	    wait_q.addLast (item);
	}
	else {
	    if (item instanceof BambooRouteUpcall) { 
		BambooRouteUpcall msg = (BambooRouteUpcall) item;
		if (msg.payload instanceof MCastJoinMsg)
		    handleMCastJoinMsg(msg, (MCastJoinMsg) msg.payload);
		else
		    BUG ("Scribe got unexpected upcall: " + item + ".");

	    }
	    else if (item instanceof BambooRouteDeliver) { 
		BambooRouteDeliver msg = (BambooRouteDeliver) item;
		if (msg.payload instanceof MCastUpMsg)
		    handleMCastUpMsg((MCastUpMsg) msg.payload);
		else if (msg.payload instanceof MCastJoinMsg)
		    handleMCastJoinMsg(null, (MCastJoinMsg) msg.payload);
		else
		    BUG ("Scribe got unexpected deliver: " + item + ".");
	    }
	    else if (item instanceof ParentAlarm) 
		handleParentAlarm((ParentAlarm) item);
	    else if (item instanceof ChildAlarm) 
		handleChildAlarm((ChildAlarm) item);
	    else if (item instanceof MCastDownMsg) 
		handleMCastDownMsg((MCastDownMsg) item);
	    else if (item instanceof MCastUp) 
		handleMCastUp((MCastUp) item);
	    else if (item instanceof MCastJoin) 
		handleMCastJoin((MCastJoin) item);
	    else
		BUG ("got event of unexpected type: " + item + ".");
	}
    }
    
    private void handleParentAlarm(ParentAlarm alarm) {
	// send a ping to my parent for every group that I'm in
	if (DEBUG) { 
	    Debug.printtagln(tag, "handleParentAlarm"); printLocalGroups(); 
	}
	Vector sendQueue = new Vector();
	for (Enumeration e = myLocalGroups.elements() ; e.hasMoreElements() ;) {
	    BigInteger bi = (BigInteger)e.nextElement();
	    MCastJoinMsg m = new MCastJoinMsg(my_node_id, bi);
	    if (DEBUG) Debug.printtagln(tag, 
		"handleParentAlarm will dispatch to " + 
		GuidTools.guid_to_string(bi) + " message "+m);
	    dispatch(new BambooRouteInit(bi, app_id, true, false, m));
	}
	classifier.dispatch_later (new ParentAlarm (), PARENT_ALARM_INTERVAL + 
				   rand.nextInt (PARENT_ALARM_VARIATION));
    }

    private void handleChildAlarm(ChildAlarm alarm) {
	// delete children we haven't heard from since the last ChildAlarm
	if (DEBUG) { 
	    Debug.printtagln(tag, "handleChildAlarm entry"); printChildTable(); 
	}
	Vector v = new Vector();
	Vector w = new Vector();
	Iterator groups = allChildGroups.keySet().iterator();
	while (groups.hasNext()) {
	    BigInteger b = (BigInteger)groups.next();
	    HashMap h = (HashMap)allChildGroups.get(b);
	    Iterator nodes = h.keySet().iterator();
	    while (nodes.hasNext()) {
		NodeId n = (NodeId)nodes.next();
		// clear flag no matter what; if already false, delete the node
		if ((((Boolean)h.put(n, new Boolean(false))).booleanValue()) == false) {
		    if (DEBUG) 
			Debug.printtagln(tag, 
			"handleChildAlarm deleting node "+n+" from group "+b);
		    v.add(n);
		}
	    }
	    for (int i = 0; i < v.size(); i++) { h.remove((NodeId)v.get(i)); }
	    if (h.isEmpty()) {
		w.add(b);
	    }
	}
	for (int i=0; i < w.size(); i++) { 
	    allChildGroups.remove((BigInteger)w.get(i)); 
	}
	if (DEBUG) { 
	    Debug.printtagln(tag, "handleChildAlarm exit"); 
	    printChildTable(); 
	}
	classifier.dispatch_later (new ChildAlarm (), CHILD_ALARM_INTERVAL + 
				   rand.nextInt(CHILD_ALARM_VARIATION));
    }
    
    private void handleMCastDownMsg(MCastDownMsg mm) {
	if (DEBUG) { 
	    Debug.printtagln(tag, "entered handleMCastDownMsg arg is "+mm); 
	    printLocalGroups(); 
	}
	// We get the inbound ones from the wire. We should only get one of
	// these if we are subscribed, unless we unsubscribed since the last
	// time we sent a parent ping. Dispatch an inbound MCastDown if we
	// subscribed.
	boolean weSubscribed = myLocalGroups.contains(mm.srcguid);
	Vector sendQueue = new Vector();
	if (weSubscribed) {
	    MCastDown m = new MCastDown(true, mm.srcguid, mm.o);
	    if (DEBUG) 
		Debug.printtagln(tag, 
		"D handleMCastDownMsg will send local event "+m+
			 " with payload as string of "+mm.o);
	    sendQueue.add(m);
	}
	// Forward this MCastDownMsg over IP to all of our children in this tree
        HashMap h = (HashMap)allChildGroups.get(mm.srcguid);
        if (h != null) { // no kids, but we may still need to send a local event
            Iterator nodes = h.keySet().iterator();
            while (nodes.hasNext()) {
                NodeId n = (NodeId)nodes.next();
                MCastDownMsg mnew = 
                    new MCastDownMsg(mm.srcguid, mm.srcid, n, mm.o);
                if (DEBUG)
                    Debug.printtagln(tag,"handleMCastDownMsg will send "+mnew);
                sendQueue.add(mnew);
            }
        }
	for (Enumeration e = sendQueue.elements() ; e.hasMoreElements() ;) {
	    Object o = e.nextElement();
	    if (o instanceof MCastDown) dispatch((MCastDown)o);
	    else if (o instanceof MCastDownMsg) dispatch((MCastDownMsg)o);
	    else BUG("handleMCastDownMsg dispatch non-MCast{Down,DownMsg}?");
	}
    }
    
    private void handleMCastUp(MCastUp u) {
	// turn it into an MCastUpMsg and send it on its way
	MCastUpMsg um = new MCastUpMsg(my_node_id, u.dstguid, u.o);
	if (DEBUG) Debug.printtagln(tag, "D handleMCastUp will send "+um+
	    "to guid "+GuidTools.guid_to_string(u.dstguid)+" payload "+um.o);
	dispatch(new BambooRouteInit(u.dstguid, app_id, false, false, um));
    }

    private void handleMCastUpMsg(MCastUpMsg payload) {

	if (DEBUG) Debug.printtagln(tag, 
		    "handleMCastUpMsg at root, incoming msg from "+payload);
	
	// turn into an MCastDownMsg. if we subscribe, it will be delivered
	// locally
	MCastDownMsg m = new MCastDownMsg(payload.dstguid, my_node_id, 
					  my_node_id, payload.o);
	if (DEBUG) Debug.printtagln(tag, 
		    "handleMCastUpMsg we are root, about to dispatch "+m);
	dispatch(m);
    }

    private void handleMCastJoin(MCastJoin j) {
	if (DEBUG) Debug.printtagln(tag, "handleMCastJoin got "+j);
	if (j.join && !(myLocalGroups.contains(j.groupguid))) {
	    // subscribe to this group
	    myLocalGroups.add(j.groupguid);
	    MCastJoinMsg m = new MCastJoinMsg(my_node_id, j.groupguid);
	    dispatch(new BambooRouteInit(j.groupguid, app_id, true, false, m));
	}
	else if (!j.join && (myLocalGroups.contains(j.groupguid))) {
	    // unsubscribe from this group
	    myLocalGroups.remove(j.groupguid);
	}
    }

    private void handleMCastJoinMsg(BambooRouteUpcall upcall, MCastJoinMsg payload) {
	if (DEBUG && upcall == null) Debug.printtagln(tag, 
			      "handleMCastJoinMsg at root, message "+payload);
	else if (DEBUG) Debug.printtagln(tag, 
			 "handleMCastJoinMsg not at root, message "+payload);
	// it's not cool to be your own child
	if (payload.srcid == my_node_id) return;
	if (myLocalGroups.contains(payload.dstguid) || upcall == null) {
	    // we are a member or root; set flag to true indicating we received
	    unionChildToGroup(payload.dstguid, payload.srcid);
	    if (DEBUG) { 
		Debug.printtagln(tag, 
			 "we are a member or root, updating our child table");
		printChildTable();
	    }
	}
	else {
	    // we are neither a member nor the root; continue the message
	    if (DEBUG) Debug.printtagln(tag, 
		"we are neither a member nor root, continuing the message");
	    dispatch(new BambooRouteContinue(upcall, payload));
	}
    }

    // Misc. fucntions

    private void unionChildToGroup(BigInteger groupguid, NodeId n) {
	//long now_ms = System.currentTimeMillis ();
	HashMap h = (HashMap)allChildGroups.get(groupguid);
	if (h == null) { // this is the first child of ours for this group
	    h = new HashMap();
	    allChildGroups.put(groupguid, h);
	}
	//h.put(n, now_ms);
	h.put(n, new Boolean(true));
    }
    private void printChildTable() {
	String s = "printChildTable // ";
	Iterator groups = allChildGroups.keySet().iterator();
	while (groups.hasNext()) {
	    BigInteger b = (BigInteger)groups.next();
	    s += GuidTools.guid_to_string(b)+" "+allChildGroups.get(b)+" // ";
	}
	Debug.printtagln(tag, s);
    }

    private void printLocalGroups() {
	String s = "printLocalGroups // ";
	for (Enumeration e = myLocalGroups.elements() ; e.hasMoreElements() ;) {
	    s += GuidTools.guid_to_string((BigInteger)e.nextElement())+" // ";
	}
	Debug.printtagln(tag, s);
    }

}
