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
 * @version $Id: LeafSetChanged.java,v 1.6 2003/10/05 18:22:11 srhea Exp $
 */
public class LeafSetChanged extends NetworkMessage {

    public LinkedList leaf_set;
    public BigInteger guid;
    public boolean want_reply;

    public LeafSetChanged (NodeId dest, BigInteger g, LinkedList l) {
	super (dest, false);  guid = g;  leaf_set = l;
    }

    public LeafSetChanged (InputBuffer buffer) throws QSException {
	super (buffer);
	guid = buffer.nextBigInteger ();
	int leaf_set_size = buffer.nextInt ();
	leaf_set = new LinkedList ();
	for (int i = 0; i < leaf_set_size; ++i) 
	    leaf_set.addLast (new NeighborInfo (buffer));
	want_reply = buffer.nextBoolean ();
    }

    public void serialize (OutputBuffer buffer) {
	super.serialize (buffer);
	buffer.add (guid);
	buffer.add (leaf_set.size ());
	for (Iterator j = leaf_set.iterator (); j.hasNext (); )
	    ((NeighborInfo) j.next ()).serialize (buffer);
	buffer.add (want_reply);
    }

    public Object clone () throws CloneNotSupportedException {
	LeafSetChanged result = (LeafSetChanged) super.clone ();
	result.guid = guid;
	result.leaf_set = leaf_set;
	result.want_reply = want_reply;
	return result;
    }

    public String toString () {
	StringBuffer result = new StringBuffer (100 + 50*leaf_set.size ());
	result.append ("(LeafSetChanged super=");
	result.append (super.toString ());
	result.append (" guid=0x");
	result.append (bamboo.util.GuidTools.guid_to_string (guid));
	result.append (" leaf_set=");
	for (Iterator j = leaf_set.iterator (); j.hasNext (); ) {
	    result.append (j.next ());
	    if (j.hasNext ()) 
	    result.append (", ");
	}
	result.append (") want_reply=");
	result.append (want_reply);
	result.append (")");
	return result.toString ();
    }
}

