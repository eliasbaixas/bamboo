/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.router;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.LinkedList;
import ostore.network.NetworkMessage;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;

/**
 * Used to proactively inform a node's neighbors when its leaf set changes.
 * 
 * @author Sean C. Rhea
 * @version $Id: RoutingNeighborAnnounce.java,v 1.3 2003/10/05 18:22:11 srhea Exp $
 */
public class RoutingNeighborAnnounce extends NetworkMessage {

    public BigInteger guid;
    public boolean add;

    public RoutingNeighborAnnounce (NodeId dest, BigInteger g, boolean a) {
	super (dest, false);  guid = g;  add = a;
    }

    public RoutingNeighborAnnounce (InputBuffer buffer) throws QSException {
	super (buffer);
	guid = buffer.nextBigInteger ();
	add = buffer.nextBoolean ();
    }

    public void serialize (OutputBuffer buffer) {
	super.serialize (buffer);
	buffer.add (guid);
	buffer.add (add);
    }

    public Object clone () throws CloneNotSupportedException {
	RoutingNeighborAnnounce result = (RoutingNeighborAnnounce) super.clone ();
	result.guid = guid;
	result.add = add;
	return result;
    }

    public String toString () {
	StringBuffer result = new StringBuffer (140);
	result.append ("(RoutingNeighborAnnounce super=");
	result.append (super.toString ());
	result.append (" guid=0x");
	result.append (bamboo.util.GuidTools.guid_to_string (guid));
	result.append (" add=");
	result.append (add);
	result.append (")");
	return result.toString ();
    }
}

