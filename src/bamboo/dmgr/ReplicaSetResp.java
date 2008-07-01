/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dmgr;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.LinkedList;
import ostore.network.NetworkMessage;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;

/**
 * ReplicaSetResp.
 *
 * @author  Sean C. Rhea
 * @version $Id: ReplicaSetResp.java,v 1.4 2004/05/18 19:12:11 srhea Exp $
 */
public class ReplicaSetResp extends NetworkMessage {

    public long nonce;
    public NodeId [] replica_set;

    public ReplicaSetResp (NodeId dest, long n, NodeId [] r) {
        super (dest, false); nonce = n; replica_set = r;
    }

    public ReplicaSetResp (InputBuffer buffer) throws QSException {
	super (buffer);
	nonce = buffer.nextLong ();
	int n = buffer.nextInt ();
        if (n <= 0) throw new QSException ("replica_set empty");
	replica_set = new NodeId [n];
	for (int i = 0; i < n; ++i) 
	    replica_set [i] = new NodeId (buffer);
    }

    public void serialize (OutputBuffer buffer) {
	super.serialize (buffer);
        buffer.add (nonce);
        buffer.add (replica_set.length);
	for (int i = 0; i < replica_set.length; ++i) 
	    replica_set [i].serialize (buffer);
    }

    public Object clone () throws CloneNotSupportedException {
        ReplicaSetResp result = (ReplicaSetResp) super.clone ();
        result.nonce = nonce;
        result.replica_set = replica_set;
        return result;
    }

    public String toString () {
	StringBuffer result = new StringBuffer (100+30*replica_set.length);
	result.append ("(ReplicaSetResp super=");
	result.append (super.toString ());
	result.append (" nonce="); result.append (nonce);
	result.append (" replica_set=(");
	for (int i = 0; i < replica_set.length; ++i) {
	    result.append (replica_set [i]);
	    if (i < replica_set.length - 1)
		result.append (" "); 
	}
	result.append ("))");
	return result.toString ();
    }
}

