/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dmgr;
import java.math.BigInteger;
import ostore.util.ByteArrayOutputBuffer;
import ostore.util.CountBuffer;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SinkIF;
import bamboo.util.GuidTools;

/**
 * ReplicaSetReq.
 * 
 * @author Sean C. Rhea
 * @version $Id: ReplicaSetReq.java,v 1.5 2004/08/04 19:23:27 srhea Exp $
 */
public class ReplicaSetReq implements QuickSerializable {

    public NodeId return_address;
    public BigInteger guid;
    public long nonce;

    public ReplicaSetReq (NodeId n, BigInteger g, long x) { 
	return_address = n; guid = g; nonce = x; 
    }

    public ReplicaSetReq (InputBuffer buffer) throws QSException { 
	return_address = new NodeId (buffer);
	guid = buffer.nextBigInteger ();
	nonce = buffer.nextLong ();
    }

    public void serialize (OutputBuffer buffer) {
	return_address.serialize (buffer);
	buffer.add (guid);
	buffer.add (nonce);
    }

    public Object clone () throws CloneNotSupportedException {
        ReplicaSetReq result = (ReplicaSetReq) super.clone ();
        result.return_address = return_address;
        result.guid = guid;
        result.nonce = nonce;
        return result;
    }

    public String toString () {
	return "(ReplicaSetReq return_address=" + return_address + 
	    " guid=" + GuidTools.guid_to_string (guid) + 
	    " nonce=" + nonce + ")";
    }
}

