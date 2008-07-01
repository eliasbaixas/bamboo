/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.router;
import bamboo.api.BambooNeighborInfo;
import bamboo.util.GuidTools;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Random;
import java.util.Set;
import ostore.util.NodeId;
import static bamboo.util.GuidTools.*;

/**
 * Manages the leaf set for node.
 * 
 * @author Sean C. Rhea
 * @version $Id: LeafSet.java,v 1.31 2005/08/16 05:11:08 srhea Exp $
 */
public class LeafSet {

    protected BigInteger MODULUS;
    
    protected BigInteger my_guid;
    protected NeighborInfo my_neighbor_info;

    protected NeighborInfo [] leaf_preds;
    protected NeighborInfo [] leaf_succs;
    protected int leaf_set_size;
    protected int leaf_pred_count, leaf_succ_count;
    protected boolean overlap;

    public boolean overlap() { return overlap; }

    public NeighborInfo random_member (Random rand) {
	if (leaf_pred_count + leaf_succ_count == 0) return null;
	int which = rand.nextInt (leaf_pred_count + leaf_succ_count);
	if (which < leaf_pred_count)
	    return leaf_preds [which];
	which -= leaf_pred_count;
	return leaf_succs [which];
    }

    public LeafSet (NeighborInfo ni, int sz, BigInteger mod) {
	MODULUS = mod;
	my_neighbor_info = ni;
	my_guid = ni.guid;
	leaf_set_size = sz;
	leaf_preds = new NeighborInfo [leaf_set_size];
	leaf_succs = new NeighborInfo [leaf_set_size];
        updateOverlap();
    }

    public LinkedList<NeighborInfo> as_list() {
	LinkedList result = new LinkedList<NeighborInfo>();
	for (int j = leaf_pred_count - 1; j >= 0; --j) 
	    result.addLast (leaf_preds [j]);
	for (int j = 0; j < leaf_succ_count; ++j) 
	    result.addLast (leaf_succs [j]);
	return result;
    }

    public BambooNeighborInfo [] preds () {
	BambooNeighborInfo [] result = new BambooNeighborInfo [leaf_pred_count];
	for (int j = 0; j < leaf_pred_count; ++j) 
	    result [j] = new BambooNeighborInfo (
		    leaf_preds [j].node_id, leaf_preds [j].guid);
	return result;
    }

    public BambooNeighborInfo [] succs () {
	BambooNeighborInfo [] result = new BambooNeighborInfo [leaf_succ_count];
	for (int j = 0; j < leaf_succ_count; ++j) 
	    result [j] = new BambooNeighborInfo (
		    leaf_succs [j].node_id, leaf_succs [j].guid);
	return result;
    }

    public Set<NeighborInfo> as_set () {
	Set<NeighborInfo> result = new LinkedHashSet<NeighborInfo>();
	for (int j = 0; j < leaf_pred_count; ++j) 
	    result.add (leaf_preds [j]);
	for (int j = 0; j < leaf_succ_count; ++j) 
	    result.add (leaf_succs [j]);
	return result;
    }

    /**
     * Returns a set of the nodes between us and the given node in our leaf
     * set, or null if none exist.
     */
    public Set<NeighborInfo> intermediates(BigInteger other) {
	Set<NeighborInfo> result = null;
	int i;
	// Is is a predecessor?
	for (i = 0; i < leaf_pred_count; ++i) {
	    if (leaf_preds [i].guid.equals (other)) 
		break;
	}
	// If so, who's between us?
	if (i < leaf_pred_count) {
	    for (i = 0; i < leaf_pred_count; ++i) {
		if (leaf_preds [i].guid.equals (other)) 
		    break;
		if (in_range_mod (other, my_guid, leaf_preds [i].guid)) {
		    if (result == null)
			result = new LinkedHashSet<NeighborInfo>();
		    result.add (leaf_preds [i]);
		}
	    }
	}
	// Is it a successor?
	for (i = 0; i < leaf_succ_count; ++i) {
	    if (leaf_succs [i].guid.equals (other)) 
		break;
	}
	// If so, who's between us?
	if (i < leaf_succ_count) {
	    for (i = 0; i < leaf_succ_count; ++i) {
		if (leaf_succs [i].guid.equals (other)) 
		    break;
		if (in_range_mod (my_guid, other, leaf_succs [i].guid)) {
		    if (result == null)
			result = new LinkedHashSet<NeighborInfo>();
		    result.add (leaf_preds [i]);
		}
	    }
	}

	return result;
    }

    /**
     * Returns the set of nodes in the DHT that should have replicas for the
     * given key.
     */
    public Set<NeighborInfo> replicas(BigInteger key, int desiredReplicas) {
        desiredReplicas = Math.min(desiredReplicas, 
                                   leaf_pred_count + leaf_succ_count);
        assert (desiredReplicas & 0x1) == 0; // better be even

        Set<NeighborInfo> result = new LinkedHashSet<NeighborInfo>();
        if (leaf_pred_count == 0) {
            result.add(my_neighbor_info);
        }
        else {
            int closestIndex = 0;
            BigInteger minDistance = calc_dist(my_guid, key);
            for (int i = 0; i < leaf_pred_count; ++i) {
                BigInteger tmp = calc_dist(leaf_preds[i].guid, key);
                if (tmp.compareTo(minDistance) < 0) {
                    closestIndex = (i + 1) * -1;
                    minDistance = tmp;
                }
            }
            for (int i = 0; i < leaf_succ_count; ++i) {
                BigInteger tmp = calc_dist(leaf_succs[i].guid, key);
                if (tmp.compareTo(minDistance) < 0) {
                    closestIndex = (i + 1);
                    minDistance = tmp;
                }
            }
            int half = desiredReplicas / 2;
            int start = 0;
            if (overlap || ((0 - closestIndex != leaf_pred_count)
                            && (closestIndex != leaf_succ_count))) {

                // If it's either edge of the leaf set, that means we actually
                // have no idea who the replicas are, so we shouldn't return
                // anything.

                if (closestIndex == 0) {
                    if (in_range_mod(leaf_preds[0].guid, my_guid, key))
                        start = closestIndex - half; 
                    else 
                        start = closestIndex - half + 1; 
                }
                else if (closestIndex < 0) {
                    int i = -1 * closestIndex - 1;
                    if (in_range_mod(leaf_preds[i].guid, my_guid, key))
                        start = closestIndex - half + 1; 
                    else 
                        start = closestIndex - half; 
                }
                else {
                    int i = closestIndex - 1;
                    if (in_range_mod(my_guid, leaf_succs[i].guid, key))
                        start = closestIndex - half; 
                    else 
                        start = closestIndex - half + 1; 
                }
                int stop = start + desiredReplicas;
                for (int i = start; i < stop; ++i) {
                    if (i == 0) {
                        result.add(my_neighbor_info);
                    }
                    else if (i < 0) {
                        int j = -1 * i - 1;
                        if (j < leaf_pred_count)
                            result.add(leaf_preds[j]);
                    }
                    else {
                        int j = i - 1;
                        if (j < leaf_succ_count)
                            result.add(leaf_succs[j]);
                    }
                }
            }
        }
        return result;
    }

    public boolean contains (NeighborInfo ni) {
	if (ni.guid.equals (my_guid))
	    return false;
	for (int i = 0; i < leaf_pred_count; ++i) 
	    if (leaf_preds [i].guid.equals (ni.guid)) 
		return true;
	for (int i = 0; i < leaf_succ_count; ++i) 
	    if (leaf_succs [i].guid.equals (ni.guid)) 
		return true;
	return false;
    }

    public boolean promising (NeighborInfo ni) {

	if (ni.guid.equals (my_guid))
	    return false;

	if (leaf_pred_count == 0) {
	    // There is no one in our leaf set.
	    return true;
	}

	boolean duplicate = false;
	for (int i = 0; i < leaf_pred_count; ++i) 
	    if (leaf_preds [i].guid.equals (ni.guid)) 
		duplicate = true;

	if (! duplicate) {
	    int i = 0;
	    for (i = 0; i < leaf_pred_count; ++i) {
		if (in_range_mod (leaf_preds [i].guid, my_guid, ni.guid)) 
		    return true;
	    }
	    if ((i == leaf_pred_count) && 
		    (leaf_pred_count < leaf_set_size)) {
		return true;
	    }
	}

	duplicate = false;
	for (int i = 0; i < leaf_succ_count; ++i) 
	    if (leaf_succs [i].guid.equals (ni.guid)) 
		duplicate = true;

	if (! duplicate) {
	    int i;
	    for (i = 0; i < leaf_succ_count; ++i) {
		if (in_range_mod (my_guid, leaf_succs [i].guid, ni.guid)) 
		    return true;
	    }
	    if ((i == leaf_succ_count) && 
		    (leaf_succ_count < leaf_set_size)) {
		return true;
	    }
	}

	return false;
    }

    public NeighborInfo add_node (NeighborInfo ni) {

	Set old_leaf_set = null;

	if (ni.guid.equals (my_guid))
	    return null;

	if (leaf_pred_count == 0) {
	    // There is no one in our leaf set.
	    leaf_preds [0] = leaf_succs [0] = ni;
	    leaf_pred_count = leaf_succ_count = 1;
            overlap = true;
	    return my_neighbor_info;
	}

	boolean duplicate = false;
	for (int i = 0; i < leaf_pred_count; ++i) 
	    if (leaf_preds [i].guid.equals (ni.guid)) 
		duplicate = true;

	if (! duplicate) {
	    int i = 0;
	    for (i = 0; i < leaf_pred_count; ++i) {

		if (in_range_mod (leaf_preds [i].guid, my_guid, ni.guid)) {

		    if (old_leaf_set == null) 
			old_leaf_set = as_set ();

		    for (int j = leaf_set_size - 1; j > i; --j)
			leaf_preds [j] = leaf_preds [j - 1];
		    leaf_preds [i] = ni;
		    if (leaf_pred_count < leaf_set_size) 
			++leaf_pred_count;

		    break;
		}
	    }
	    if ((i == leaf_pred_count) && 
		    (leaf_pred_count < leaf_set_size)) {

		if (old_leaf_set == null) 
		    old_leaf_set = as_set ();

		leaf_preds [leaf_pred_count] = ni;
		++leaf_pred_count;
	    }
	}

	duplicate = false;
	for (int i = 0; i < leaf_succ_count; ++i) 
	    if (leaf_succs [i].guid.equals (ni.guid)) 
		duplicate = true;

	if (! duplicate) {
	    int i;
	    for (i = 0; i < leaf_succ_count; ++i) {

		if (in_range_mod (my_guid, leaf_succs [i].guid, ni.guid)) {

		    if (old_leaf_set == null) 
			old_leaf_set = as_set ();

		    for (int j = leaf_set_size - 1; j > i; --j)
			leaf_succs [j] = leaf_succs [j - 1];
		    leaf_succs [i] = ni;
		    if (leaf_succ_count < leaf_set_size) 
			++leaf_succ_count;

		    break;
		}
	    }
	    if ((i == leaf_succ_count) && 
		    (leaf_succ_count < leaf_set_size)) {

		if (old_leaf_set == null) 
		    old_leaf_set = as_set ();

		leaf_succs [leaf_succ_count] = ni;
		++leaf_succ_count;
	    }
	}

	if (old_leaf_set != null) {

            // The leaf set has changed.

            updateOverlap();

	    old_leaf_set.removeAll (as_set ());
	    if (old_leaf_set.isEmpty ())
		return my_neighbor_info;

	    if (old_leaf_set.size () != 1) {
                System.err.println ("Error adding " + ni);
                System.err.println ("old_leaf_set=");
                Iterator i = old_leaf_set.iterator ();
                while (i.hasNext ()) {
                    NeighborInfo other = (NeighborInfo) i.next ();
                    System.err.println ("  " + other);
                }
                System.err.println ("leaf_set=");
                i = as_set ().iterator ();
                while (i.hasNext ()) {
                    NeighborInfo other = (NeighborInfo) i.next ();
                    System.err.println ("  " + other);
                }
                assert false;
            }
	    return (NeighborInfo) old_leaf_set.iterator ().next ();
	}

	return null;
    }

    public static final int REMOVED_NONE        = 0x0;
    public static final int REMOVED_PREDECESSOR = 0x1;
    public static final int REMOVED_SUCCESSOR   = 0x2;
    public static final int REMOVED_BOTH        = 0x3;

    public int remove_node (NeighborInfo ni) {

	int result = 0;

	int i;
	for (i = 0; i < leaf_pred_count; ++i) {
	    if (leaf_preds [i].guid.equals (ni.guid)) 
		break;
	}
	if (i != leaf_pred_count) {
	    // removing the node at leaf_preds [i]
	    result |= REMOVED_PREDECESSOR;
	    for (; i < leaf_pred_count - 1; ++i) 
		leaf_preds [i] = leaf_preds [i+1];
	    --leaf_pred_count;
	}

	for (i = 0; i < leaf_succ_count; ++i) {
	    if (leaf_succs [i].guid.equals (ni.guid)) 
		break;
	}
	if (i != leaf_succ_count) {
	    // removing the node at leaf_succs [i]
	    result |= REMOVED_SUCCESSOR;
	    for (; i < leaf_succ_count - 1; ++i) 
		leaf_succs [i] = leaf_succs [i+1];
	    --leaf_succ_count;
	}

        updateOverlap();
	if (overlap) {
	    // TODO: optimize?
	    for (i = 0; i < leaf_pred_count; ++i)
		add_node (leaf_preds [i]);
	    for (i = 0; i < leaf_succ_count; ++i) 
		add_node (leaf_succs [i]);
	}

	return result;
    }

    protected void updateOverlap() {
        if (leaf_pred_count == 0 || leaf_succ_count == 0) {
            overlap = true;
            return;
        }
        overlap = false;
        for (int i = 0; i < leaf_pred_count; ++i) {
            for (int j = 0; j < leaf_succ_count; ++j) {
                if (leaf_preds [i].guid.equals (leaf_succs [j].guid)) {
                    overlap = true;
                    break;
                }
            }
        }
    }

    public String toString () {
	StringBuffer result = new StringBuffer (500);
	for (int i = leaf_pred_count - 1; i >= 0; --i) {
	    result.append ("  ");
	    result.append (0-i-1);
	    result.append ('\t');
	    result.append (leaf_preds [i]);
	    result.append ('\n');
	}
	result.append ("  ");
	result.append (0);
	result.append ('\t');
	result.append (my_neighbor_info);
	result.append ('\n');
	for (int i = 0; i < leaf_succ_count; ++i) {
	    result.append ("  ");
	    result.append (i+1);
	    result.append ('\t');
	    result.append (leaf_succs [i]);
	    result.append ('\n');
	}
	return result.toString ();
    }

    public BigInteger calc_dist (BigInteger a, BigInteger b) {
        return GuidTools.calc_dist (a, b, MODULUS);
    }

    public NeighborInfo closest_leaf (BigInteger guid, Set ignore) {
	NeighborInfo closest = my_neighbor_info;
	BigInteger dist = calc_dist (my_neighbor_info.guid, guid);
	for (int j = 0; j < leaf_pred_count; j++) {
	    NeighborInfo ni = leaf_preds [j];
            if (ignore.contains (ni))
                continue;
	    BigInteger this_dist = calc_dist (ni.guid, guid);
	    if (this_dist.compareTo (dist) < 0) {
		closest = ni;
		dist = this_dist;
	    }
	    else if ((this_dist.compareTo (dist) == 0) && 
	             (! ni.equals (closest)) &&
		     (in_range_mod (closest.guid, ni.guid, guid) &&
		     (! in_range_mod (ni.guid, closest.guid, guid))))
	    {
		// picks the leaf_succ in case of tie
		closest = ni;
		dist = this_dist;
	    }
	}
	for (int j = 0; j < leaf_succ_count; j++) {
	    NeighborInfo ni = leaf_succs [j];
            if (ignore.contains (ni))
                continue;
	    BigInteger this_dist = calc_dist (ni.guid, guid);
	    if (this_dist.compareTo (dist) < 0) {
		closest = ni;
		dist = this_dist;
	    }
	    else if ((this_dist.compareTo (dist) == 0) && 
	             (! ni.equals (closest)) &&
		     (in_range_mod (closest.guid, ni.guid, guid) &&
		     (! in_range_mod (ni.guid, closest.guid, guid))))
	    {
		// picks the leaf_succ in case of tie
		closest = ni;
		dist = this_dist;
	    }
	}
	return closest;
    }

    public final boolean within_leaf_set (BigInteger i) {
	return in_range_mod (leaf_set_low (), my_guid, i) ||
	       in_range_mod (my_guid, leaf_set_high (), i);
    }

    public final BigInteger leaf_set_low () {
	if (leaf_pred_count == 0)
	    return my_guid;
	else 
	    return leaf_preds [leaf_pred_count - 1].guid;
    }

    public final BigInteger leaf_set_high () {
	if (leaf_succ_count == 0)
	    return my_guid;
	else 
	    return leaf_succs [leaf_succ_count - 1].guid;
    }

    protected boolean in_range_mod (
	    BigInteger low, BigInteger high, BigInteger query) {
	return GuidTools.in_range_mod (low, high, query, MODULUS);
    }
}

