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
import java.util.Set;
import java.util.TreeSet;
import ostore.network.NetworkMessage;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;

/**
 * JoinResp.
 *
 * @author  Sean C. Rhea
 * @version $Id: JoinResp.java,v 1.6 2003/10/05 18:22:11 srhea Exp $
 */
public class JoinResp extends NetworkMessage {

    public LinkedList path = new LinkedList ();
    public Set leaf_set = new TreeSet ();

    public JoinResp (NodeId dest, LinkedList p, Set l) {
	super (dest, false);
	path = p;
	leaf_set = l;
    }

    public JoinResp (InputBuffer buffer) throws QSException {
	super (buffer);
	int path_len = buffer.nextInt ();
	while (path_len-- > 0) 
	    path.addLast (new NeighborInfo (buffer));
	path_len = buffer.nextInt ();
	leaf_set = new TreeSet ();
	while (path_len-- > 0) 
	    leaf_set.add (new NeighborInfo (buffer));
    }

    public void serialize (OutputBuffer buffer) {
	super.serialize (buffer);
	buffer.add (path.size ());
	for (Iterator i = path.iterator (); i.hasNext (); ) 
	    ((NeighborInfo) i.next ()).serialize (buffer);
	buffer.add (leaf_set.size ());
	for (Iterator i = leaf_set.iterator (); i.hasNext (); ) 
	    ((NeighborInfo) i.next ()).serialize (buffer);
    }

    public Object clone () throws CloneNotSupportedException {
	JoinResp result = (JoinResp) super.clone ();
	result.path = new LinkedList ();
	for (Iterator i = path.iterator (); i.hasNext (); ) 
	    result.path.addLast ((NeighborInfo) i.next ());
	result.leaf_set = new TreeSet ();
	for (Iterator i = leaf_set.iterator (); i.hasNext (); ) 
	    result.leaf_set.add ((NeighborInfo) i.next ());
	return result;
    }

    public String toString () {
	StringBuffer result = 
	    new StringBuffer (50 + 70 * path.size () + 70 * leaf_set.size ());
	result.append ("(JoinResp super=");
	result.append (super.toString ());
	result.append (" path=(");
	for (Iterator i = path.iterator (); i.hasNext (); ) {
	    result.append ((NeighborInfo) i.next ());
	    if (i.hasNext ()) result.append (", ");
	}
	result.append (") leaf_set=(");
	for (Iterator i = leaf_set.iterator (); i.hasNext (); ) {
	    result.append ((NeighborInfo) i.next ());
	    if (i.hasNext ()) result.append (", ");
	}
	result.append ("))");
	return result.toString ();
    }
}

