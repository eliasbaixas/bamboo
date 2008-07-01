/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.router;

import java.math.BigInteger;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.Random;
import java.io.FileInputStream;
import ostore.util.QuickSerializable;
import ostore.util.Carp;
import ostore.util.Debug;
import ostore.util.DebugFlags;
import ostore.util.NodeId;
import ostore.util.Pair;
import ostore.util.QSIO;
import ostore.util.SecureHash;
import ostore.util.SHA1Hash;
import bamboo.util.StandardStage;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.EventHandlerException;
import seda.sandStorm.api.EventHandlerIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SinkException;
import seda.sandStorm.api.SinkIF;
import seda.sandStorm.api.StagesInitializedSignal;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import seda.sandStorm.api.QueueElementIF;
import bamboo.api.BambooRouteDeliver;
import bamboo.api.BambooRouteInit;
import bamboo.api.BambooRouteContinue;
import bamboo.api.BambooRouteUpcall;
import bamboo.api.BambooRouterAppRegReq;
import bamboo.api.BambooRouterAppRegResp;
import bamboo.api.BambooNeighborInfo;
import bamboo.api.BambooLeafSetChanged;
import bamboo.util.GuidTools;

/**
 * Prints the leaf sets of a ring, given a gateway node.
 * 
 * @author Sean C. Rhea
 * @version $Id: PrintLeafSets.java,v 1.15 2004/02/13 04:28:01 srhea Exp $
 */
public class PrintLeafSets extends StandardStage 
implements SingleThreadedEventHandlerIF {

    protected NodeId gateway;
    protected Map done = new TreeMap ();
    protected Map info = new TreeMap ();
    protected Set pending = new TreeSet ();

    public PrintLeafSets () throws Exception {
	ostore.util.TypeTable.register_type (NeighborInfo.class);
	event_types   = new Class [] { StagesInitializedSignal.class };
	inb_msg_types = new Class [] { LeafSetChanged.class };
    }

    public void init(ConfigDataIF config) throws Exception {
	super.init (config);
	gateway = new NodeId (config.getString ("gateway"));
    }

    public void handleEvent (QueueElementIF item) {
	if (logger.isDebugEnabled ()) logger.debug ("got " + item);

	if (item instanceof StagesInitializedSignal) {
	    System.out.println ("Tapestry: ready");
	    pending.add (gateway);
	    System.out.println ("Asking " + gateway + " for leaf set");
	    dispatch (new LeafSetReq (gateway));
	}
	else if (item instanceof LeafSetChanged) {
	    LeafSetChanged resp = (LeafSetChanged) item;
	    System.out.println ("Got leaf set from " + resp.peer);
	    done.put (resp.peer, resp.leaf_set);
	    pending.remove (resp.peer);
	    for (Iterator i = resp.leaf_set.iterator (); i.hasNext (); ) {
		NeighborInfo ni = (NeighborInfo) i.next ();
		info.put (ni.node_id, ni);
		if ((! pending.contains (ni.node_id)) && 
		    (! done.containsKey (ni.node_id))) {
		    // New node; go get its leaf set.
		    pending.add (ni.node_id);
		    System.out.println (
			    "Asking " + ni.node_id + " for leaf set");
		    dispatch (new LeafSetReq (ni.node_id));
		}
	    }

	    if (pending.isEmpty ()) {
		// We have the leaf sets for all the nodes we are aware of.
		// Print them out.
		for (Iterator i = done.keySet ().iterator (); i.hasNext (); ) {
		    NodeId n = (NodeId) i.next ();
		    LinkedList leaf_set = (LinkedList) done.get (n);
		    System.out.println ("Leaf set for node " + n);
		    int k = 0 - leaf_set.size () / 2;
		    for (Iterator j = leaf_set.iterator (); j.hasNext (); ) {
			if (k == 0) {
			    System.out.println (
				    "  " + k++ + "\t" + info.get (n));
			}
			System.out.println ("  " + k++ + "\t" + j.next ());
		    }
		    System.out.println ("");
		}
		System.exit (0);
	    }
	    else {
		Iterator i = pending.iterator ();
		NodeId n = (NodeId) i.next ();
		System.out.println ("Still need leaf set from " + n);
	    }
	}
	else {
            throw new IllegalArgumentException (item.getClass ().getName ());
	}
    }
}

