/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dmgr;
import ostore.network.NetworkMessage;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;

/**
 * FetchMerkleTreeNodeReject.
 *
 * @author  Sean C. Rhea
 * @version $Id: FetchMerkleTreeNodeReject.java,v 1.4 2004/04/20 19:23:17 srhea Exp $
 */
public class FetchMerkleTreeNodeReject extends NetworkMessage {

    public static final int BAD_GUID_RANGE = 0;
    public static final int BAD_EXPANSION  = 1;
    public static final int NO_SUCH_NODE   = 2;

    public static final String [] reason_to_string = {
        "bad guid range", "bad expansion factor", "no such node"
    };

    /**
     * Why we rejected it.
     */
    public int reason;

    /**
     * To pair it up with its request.
     */
    public long seq;

    public FetchMerkleTreeNodeReject (NodeId dest, int r, long s) {
	super (dest, false);
        reason = r; seq = s;
    }

    public FetchMerkleTreeNodeReject (InputBuffer buffer) throws QSException {
	super (buffer);
	reason = buffer.nextInt ();
        if (reason >= reason_to_string.length)
            throw new QSException ("unknown reason " + reason);
	seq = buffer.nextLong ();
    }

    public void serialize (OutputBuffer buffer) {
	super.serialize (buffer);
        buffer.add (reason);
        buffer.add (seq);
    }

    public Object clone () throws CloneNotSupportedException {
        FetchMerkleTreeNodeReject result =
            (FetchMerkleTreeNodeReject) super.clone ();
        result.reason = reason;
        result.seq = seq;
        return result;
    }

    public String toString () {
	StringBuffer result = new StringBuffer (100);
	result.append ("(FetchMerkleTreeNodeReject super=");
	result.append (" reason=\""); result.append (reason_to_string [reason]);
	result.append ("\" seq="); result.append (seq);
	result.append (")");
	return result.toString ();
    }
}

