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
 * RoutingTableResp.
 *
 * @author  Sean C. Rhea
 * @version $Id: RoutingTableResp.java,v 1.8 2003/12/29 18:10:43 srhea Exp $
 */
public class RoutingTableResp extends NetworkMessage {

    public BigInteger peer_guid;
    public LinkedList neighbors;

    public RoutingTableResp (NodeId dest, BigInteger g, LinkedList n) {
	super (dest, false);
        peer_guid = g;
	neighbors = n;
    }

    public RoutingTableResp (InputBuffer buffer) throws QSException {
	super (buffer);
        peer_guid = buffer.nextBigInteger ();
	int count = buffer.nextInt ();
	if (count <= 0)
            return;
        else if (count > 16) {
            System.err.println ("Got RoutingTableResp with neighbors.size ()"
                    + " = " + count);
            throw new QSException ("neighbors.size () = " + count);
        }
        else {
            neighbors = new LinkedList ();
            while (count-- > 0) 
                neighbors.addLast (new NeighborInfo (buffer));
        }
    }

    public void serialize (OutputBuffer buffer) {
	super.serialize (buffer);
        buffer.add (peer_guid);
	if (neighbors == null) 
	    buffer.add (0);
	else {
	    buffer.add (neighbors.size ());
	    for (Iterator i = neighbors.iterator (); i.hasNext (); ) 
		((NeighborInfo) i.next ()).serialize (buffer);
	}
    }

    public Object clone () throws CloneNotSupportedException {
	RoutingTableResp result = (RoutingTableResp) super.clone ();
        result.peer_guid = peer_guid;
	if (neighbors != null) {
	    result.neighbors = new LinkedList ();
	    for (Iterator i = neighbors.iterator (); i.hasNext (); ) 
		result.neighbors.addLast ((NeighborInfo) i.next ());
	}
	return result;
    }

    public String toString () {
	StringBuffer result = new StringBuffer (
		50 + 70 * ((neighbors == null) ? 0 : neighbors.size ()));
	result.append ("(RoutingTableResp super=");
	result.append (super.toString ());
	result.append (" peer_guid=");
	result.append (peer_guid);
	result.append (" neighbors=(");
	if (neighbors != null) {
	    for (Iterator i = neighbors.iterator (); i.hasNext (); ) {
		result.append ((NeighborInfo) i.next ());
		if (i.hasNext ()) result.append (", ");
	    }
	}
	result.append ("))");
	return result.toString ();
    }
}

