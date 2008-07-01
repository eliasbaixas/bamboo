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
import ostore.util.QuickSerializable;
import ostore.util.Carp;
import ostore.util.Debug;
import ostore.util.DebugFlags;
import ostore.util.NodeId;
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
import bamboo.util.GuidTools;

/**
 * Abstracts the routing table for use by the Router.
 * 
 * @author Sean C. Rhea
 * @version $Id: RoutingTable.java,v 1.29 2005/12/15 21:16:14 srhea Exp $
 */
public class RoutingTable {

    protected BigInteger MODULUS;
    protected int GUID_DIGITS;
    protected int DIGIT_VALUES;
    protected int BITS_PER_DIGIT;
    protected double SCALE;

    public static class RoutingEntry {
	public NeighborInfo ni;
	public double rtt_ms;
	public RoutingEntry (NeighborInfo n, double r) {
	    ni = n;  rtt_ms = r;
	}
        public String toString () {
            return "(RoutingEntry " + ni + ", " + rtt_ms + " ms)";
        }
    }

    protected RoutingEntry [][] table;
    protected NeighborInfo my_neighbor_info;
    protected RoutingEntry my_routing_entry;
    protected int [] my_digits;
    protected int size;
    protected int highest_level;

    public int highest_level () {
        return highest_level;
    }

    public int size () {
	return size;
    }

    public BigInteger digits_to_guid (int [] digits) {
        return GuidTools.digits_to_guid (
                digits, BITS_PER_DIGIT, GUID_DIGITS, DIGIT_VALUES);
    }

    public int [] guid_to_digits (BigInteger guid) {
        return GuidTools.guid_to_digits (
                guid, BITS_PER_DIGIT, GUID_DIGITS, DIGIT_VALUES);
    }

    public RoutingTable (NeighborInfo ni, double scale, BigInteger mod,
	    int guid_digits, int digit_values) {
	MODULUS = mod;
	GUID_DIGITS = guid_digits;
	DIGIT_VALUES = digit_values;
        BITS_PER_DIGIT = 0;
        for (int i = 1; i < DIGIT_VALUES; i <<= 1) 
            ++BITS_PER_DIGIT;
        SCALE = scale;
        highest_level = -1;
	my_neighbor_info = ni;
	my_routing_entry = new RoutingEntry (ni, 0.0);
	table = new RoutingEntry [GUID_DIGITS] [];
	for (int i = 0; i < GUID_DIGITS; ++i) 
	    table [i] = new RoutingEntry [DIGIT_VALUES];
	my_digits = guid_to_digits (my_neighbor_info.guid);
	for (int i = 0; i < GUID_DIGITS; ++i) 
	    table [i][my_digits [i]] = my_routing_entry;
    }

    protected int calc_first_diff (int [] other) {
	int fd = 0;
	while ((fd < GUID_DIGITS) && 
		(my_digits [fd] == other [fd]))
	    ++fd;
	return fd;
    }

    /**
     * Returns the primary neighbor for this entry, or null if there is a
     * hole.
     */
    public NeighborInfo primary (int digit, int value) {
	if (table[digit][value] == null) 
	    return null;
	else
	    return table[digit][value].ni;
    }

    public RoutingEntry primary_re (int digit, int value) {
        return table[digit][value];
    }

    public NeighborInfo random_neighbor (int digit, Random rand) {
	int count = 0;
	for (int i = 0; i < DIGIT_VALUES; ++i) {
	    if ((table [digit][i] != null) &&
                (table [digit][i] != my_routing_entry)) {
		++count;
	    }
	}

	if (count == 0)
	    return null;

	int which = rand.nextInt () % count;
	if (which < 0) 
	    which += count; // java returns neg for some % ops

	for (int i = 0; i < DIGIT_VALUES; ++i) {
	    if ((table [digit][i] != null) &&
		(table [digit][i] != my_routing_entry)) {
		if (which == 0)
		    return table [digit][i].ni;
		--which;
	    }
	}

	Carp.die ("BUG");
	return null;
    }

    public int matching_digits (BigInteger guid) {
        return calc_first_diff (guid_to_digits (guid));
    }

    public boolean contains (NeighborInfo ni) {
	int [] dest_digits = guid_to_digits (ni.guid);
	int fd = calc_first_diff (dest_digits);
	if (fd == GUID_DIGITS)  
	    return false;

	if ((primary (fd, dest_digits [fd]) != null) &&
	    (primary (fd, dest_digits [fd]).equals (ni))) {
	    return true;
	}

	return false;
    }

    /**
     * If the node is of no use as a neighbor, it is not added and null is
     * returned; if it replaces an existing neighbor who is in turn removed,
     * that neighbor is returned; if it is added, but does not replace any
     * existing neighbor, then my_neigbor_info is returned.
     */
    public NeighborInfo add (NeighborInfo ni, double rtt_ms, 
                             boolean pns, long now_ms) {

	int [] dest_digits = guid_to_digits (ni.guid);
	int fd = calc_first_diff (dest_digits);
	if (fd == GUID_DIGITS)  
	    return null;

        NeighborInfo result = null;
	if (primary (fd, dest_digits [fd]) == null) {
	    RoutingEntry re = new RoutingEntry (ni, rtt_ms);
	    table[fd][dest_digits [fd]] = re;
	    ++size;
	    result = my_neighbor_info;
	}
        else if (primary (fd, dest_digits [fd]).equals (ni)) {
            // Update the RTT
            // System.out.println ("updating " + ni + " rtt to " + rtt_ms);
            primary_re (fd, dest_digits [fd]).rtt_ms = rtt_ms;
        }
	else if (pns && 
                 (rtt_ms < SCALE * primary_re (fd, dest_digits [fd]).rtt_ms)) {
            /*System.err.println ("old_rtt=" + 
                    primary_re (fd, dest_digits [fd]).rtt_ms + 
                    " new_rtt=" + rtt_ms);*/
	    RoutingEntry re = new RoutingEntry (ni, rtt_ms);
	    RoutingEntry removed = table[fd][dest_digits [fd]];
	    table[fd][dest_digits [fd]] = re;
	    result = removed.ni;
	}

        if (fd > highest_level)
            highest_level = fd;

	return result;
    }

    public void force_add (NeighborInfo ni, double rtt_ms) {
	int [] dest_digits = guid_to_digits (ni.guid);
	int fd = calc_first_diff (dest_digits);
	if (fd == GUID_DIGITS)  
	    throw new IllegalArgumentException ("can't force_add self");
        if(table[fd][dest_digits[fd]] == null)
            ++size;

	RoutingEntry re = new RoutingEntry (ni, rtt_ms);
	table[fd][dest_digits [fd]] = re;

        if (fd > highest_level)
            highest_level = fd;
    }

    public boolean fills_hole (NeighborInfo ni) {
	int [] dest_digits = guid_to_digits (ni.guid);
	int fd = calc_first_diff (dest_digits);
	if (fd == GUID_DIGITS)  
	    return false;
	return primary (fd, dest_digits [fd]) == null;
    }
	
    /**
     * Returns the level of the routing table from which the node was
     * removed, or -1 if the node was not in the routing table.
     */
    public int remove (NeighborInfo ni) {
	int [] dest_digits = guid_to_digits (ni.guid);
	int fd = calc_first_diff (dest_digits);
	if (fd == GUID_DIGITS)  
	    return -1;

	if ((primary (fd, dest_digits [fd]) != null) &&
	    (primary (fd, dest_digits [fd]).equals (ni))) {
	    table[fd][dest_digits [fd]] = null;
	    --size;

            if (fd == highest_level) {
                // We may need to decrease highest_level.
                int i;
                done:
                for (i = fd; i >= 0; --i) {
                    for (int j = 0; j < DIGIT_VALUES; ++j) {
                        if ((table[i][j] != null) &&
                            (table[i][j] != my_routing_entry))
                            break done;
                    }
                }
                highest_level = i;
            }

	    return fd;
	}

	return -1;
    }

    public NeighborInfo next_hop (BigInteger guid, Set ignore) {
	int [] dest_digits = guid_to_digits (guid);
        int fd = calc_first_diff (dest_digits);
        if (fd == GUID_DIGITS) 
            return my_neighbor_info;
        else {
            NeighborInfo result = primary (fd, dest_digits [fd]);
            if (ignore.contains (result))
                return null;
            else 
                return result;
        }
    }

    public LinkedList<NeighborInfo> as_list () {
        LinkedList<NeighborInfo> result = new LinkedList<NeighborInfo>();
    	for (int digit = 0; digit < GUID_DIGITS; ++digit) {
	    for (int value = 0; value < DIGIT_VALUES; ++value) {
		if ((table [digit][value] != null) &&
		    (table [digit][value] != my_routing_entry)) {
		    result.addLast (table [digit][value].ni);
		}
	    }
	}
        return result;
    }

    protected boolean GRAPHVIZ = true;

    public String toString () {
        if (GRAPHVIZ) {
            java.text.NumberFormat n = java.text.NumberFormat.getInstance();
            n.setMinimumFractionDigits(1);

            StringBuffer result = new StringBuffer (50+size*50);
            result.append (GuidTools.guid_to_string (my_routing_entry.ni.guid));
            result.append (" RT START\n");
            for (int digit = 0; digit < GUID_DIGITS; ++digit) {
                boolean first = true;
                for (int value = 0; value < DIGIT_VALUES; ++value) {
                    if ((table [digit][value] != null) &&
                            (table [digit][value] != my_routing_entry)) {

                        result.append (GuidTools.guid_to_string (
                                    my_routing_entry.ni.guid));
                        result.append (" -> ");
                        result.append (GuidTools.guid_to_string (
                                    table [digit][value].ni.guid));
                        result.append (" [ label = \"L");
                        result.append (digit);
                        result.append (", ");
                        result.append (n.format (table [digit][value].rtt_ms));
                        result.append (" ms\" ];\n");
                    }
                }
            }
            result.append (GuidTools.guid_to_string (my_routing_entry.ni.guid));
            result.append (" RT END\n");
            return result.toString ();
        }
        else {
            java.text.NumberFormat n = java.text.NumberFormat.getInstance();
            n.setMinimumFractionDigits(3);

            StringBuffer result = new StringBuffer (500);
            for (int digit = 0; digit < GUID_DIGITS; ++digit) {
                boolean first = true;
                for (int value = 0; value < DIGIT_VALUES; ++value) {
                    if ((table [digit][value] != null) &&
                            (table [digit][value] != my_routing_entry)) {

                        if (first) {
                            first = false;
                            result.append ("  Level ");
                            result.append (digit);
                            result.append (":\n");
                        }
                        result.append ("    ");
                        result.append (table [digit][value].ni);
                        result.append (" at ");
                        result.append (n.format (table [digit][value].rtt_ms));
                        result.append (" ms\n");
                    }
                }
            }
            return result.toString ();
        }
    }
}

