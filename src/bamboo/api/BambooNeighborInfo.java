/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.api;
import java.math.BigInteger;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;
import bamboo.util.GuidTools;

/**
 * BambooNeighborInfo.
 *
 * @author  Sean C. Rhea
 * @version $Id: BambooNeighborInfo.java,v 1.1 2003/10/05 19:02:02 srhea Exp $
 */
public class BambooNeighborInfo implements Comparable, QuickSerializable {

    public NodeId node_id;
    public BigInteger guid;
    public /* transient */ double rtt_ms;

    public BambooNeighborInfo (NodeId n, BigInteger g, double r) {
	node_id = n; guid = g;  rtt_ms = r;
    }

    public BambooNeighborInfo (NodeId n, BigInteger g) {
	node_id = n; guid = g;  rtt_ms = Double.MAX_VALUE;
    }

    public BambooNeighborInfo (InputBuffer buffer) throws QSException {
	node_id = (NodeId) buffer.nextObject ();
	guid = buffer.nextBigInteger ();
    }

    public void serialize (OutputBuffer buffer) {
        buffer.add (node_id);
        buffer.add (guid);
    }

    public boolean equals (Object rhs) {
	if (! (rhs instanceof BambooNeighborInfo))
	    return false;
	BambooNeighborInfo other = (BambooNeighborInfo) rhs;
	return node_id.equals (other.node_id) && guid.equals (other.guid);
    }

    public int compareTo (Object rhs) {
	BambooNeighborInfo other = (BambooNeighborInfo) rhs;
	int i;
	if ((i = node_id.compareTo (other.node_id)) != 0)
	    return i;
	return guid.compareTo (other.guid);
    }

    public int hashCode () {
	return node_id.hashCode () ^ guid.hashCode ();
    }

    public String toString () {
	return "(" + node_id.address ().getHostAddress () + ":" + 
	    node_id.port () + ", 0x" + GuidTools.guid_to_string (guid) + 
	    ((rtt_ms == Double.MAX_VALUE) ? "" : (", " + rtt_ms + " ms")) + 
	    ")";
    }
}

