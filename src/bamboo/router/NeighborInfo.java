/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.router;
import java.math.BigInteger;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;
import bamboo.util.GuidTools;

/**
 * NeighborInfo.
 *
 * @author  Sean C. Rhea
 * @version $Id: NeighborInfo.java,v 1.9 2003/10/05 18:22:11 srhea Exp $
 */
public class NeighborInfo implements Comparable, QuickSerializable {

    public NodeId node_id;
    public BigInteger guid;

    public NeighborInfo (NodeId n, BigInteger g) {
	node_id = n; guid = g;
    }

    public NeighborInfo (InputBuffer buffer) throws QSException {
	node_id = new NodeId (buffer);
	guid = buffer.nextBigInteger ();
    }

    public void serialize (OutputBuffer buffer) {
        node_id.serialize (buffer);
        buffer.add (guid);
    }

    public boolean equals (Object rhs) {
	NeighborInfo other = (NeighborInfo) rhs;
	return node_id.equals (other.node_id) && guid.equals (other.guid);
    }

    public int compareTo (Object rhs) {
	NeighborInfo other = (NeighborInfo) rhs;
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
	    node_id.port () + ", 0x" + GuidTools.guid_to_string (guid) + ")";
    }
}

