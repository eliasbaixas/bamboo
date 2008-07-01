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
 * JoinReq.
 *
 * @author  Sean C. Rhea
 * @version $Id: JoinReq.java,v 1.7 2005/03/02 03:25:45 srhea Exp $
 */
public class JoinReq extends NetworkMessage {

    public NodeId node_id;
    public BigInteger guid;
    public int rev_ttl;
    public LinkedList<NeighborInfo> path = new LinkedList<NeighborInfo>();

    public JoinReq (NodeId dest, NodeId n, BigInteger g, int r) {
	super (dest, false);  node_id = n;  guid = g;  rev_ttl = r;
    }

    public JoinReq (InputBuffer buffer) throws QSException {
	super (buffer);
	node_id = new NodeId (buffer);
	guid = buffer.nextBigInteger ();
	rev_ttl = buffer.nextInt ();
	int path_len = buffer.nextInt ();
	while (path_len-- > 0) 
	    path.addLast (new NeighborInfo (buffer));
    }

    public void serialize (OutputBuffer buffer) {
	super.serialize (buffer);
        node_id.serialize (buffer);
        buffer.add (guid);
        buffer.add (rev_ttl);
	buffer.add (path.size ());
	for (Iterator i = path.iterator (); i.hasNext (); ) 
	    ((NeighborInfo) i.next ()).serialize (buffer);
    }

    public Object clone () throws CloneNotSupportedException {
	JoinReq result = (JoinReq) super.clone ();
	result.node_id = node_id;
	result.guid = guid;
	result.rev_ttl = rev_ttl;
	result.path = new LinkedList<NeighborInfo>();
	for (Iterator i = path.iterator (); i.hasNext (); ) 
	    result.path.addLast ((NeighborInfo) i.next ());
	return result;
    }

    public String toString () {
	StringBuffer result = new StringBuffer (50 + 70 * path.size ());
	result.append ("(JoinReq super=");
	result.append (super.toString ());
	result.append (" node_id=");
	result.append (node_id);
	result.append (" guid=0x");
	result.append (bamboo.util.GuidTools.guid_to_string (guid));
	result.append (" rev_ttl=");
	result.append (rev_ttl);
	result.append (" path=(");
	for (Iterator i = path.iterator (); i.hasNext (); ) {
	    result.append ((NeighborInfo) i.next ());
	    if (i.hasNext ()) result.append (", ");
	}
	result.append ("))");
	return result.toString ();
    }
}

