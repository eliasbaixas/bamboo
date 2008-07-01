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
import ostore.util.SHA1Hash;
import bamboo.util.GuidTools;

/**
 * FetchMerkleTreeNodeReq.
 *
 * @author  Sean C. Rhea
 * @version $Id: FetchMerkleTreeNodeReq.java,v 1.5 2004/02/10 21:24:04 srhea Exp $
 */
public class FetchMerkleTreeNodeReq extends NetworkMessage {

    /**
     * The guid of the source node of this query.
     */
    public BigInteger peer_guid;

    /**
     * The lower bound of the shared database.
     */
    public BigInteger low_guid;
    
    /**
     * The upper bound of the shared database.
     */
    public BigInteger high_guid;

    /**
     * The expansion factor of the tree.  For error checking only--we expect
     * all nodes to use the same expansion factor.
     */
    public int expansion;

    /**
     * The level in the tree at which the node resides.  
     */
    public int level;

    /**
     * The lower bound of the times covered by this node
     */
    protected long low_time;

    /**
     * Our hash for this node.
     */
    public byte [] expected_hash;

    /**
     * To pair it up with its response.
     */
    public long seq;

    public FetchMerkleTreeNodeReq (NodeId dest, BigInteger pg, BigInteger lg, 
            BigInteger hg, int exp, int lv, long lt, byte [] eh, long s) {
	super (dest, false);
        peer_guid = pg; low_guid = lg; high_guid = hg; expansion = exp; 
        level = lv; low_time = lt; expected_hash = eh; seq = s;
    }

    public FetchMerkleTreeNodeReq (InputBuffer buffer) throws QSException {
	super (buffer);
	peer_guid = buffer.nextBigInteger (); 
	low_guid = buffer.nextBigInteger (); 
	high_guid = buffer.nextBigInteger ();
	expansion = buffer.nextInt ();
	level = buffer.nextInt ();
        if (level < 0) 
            throw new QSException ("level=" + level);
	low_time = buffer.nextLong ();
	int len = buffer.nextInt ();
        expected_hash = new byte [len];
        buffer.nextBytes (expected_hash, 0, len);
	seq = buffer.nextLong ();
    }

    public void serialize (OutputBuffer buffer) {
	super.serialize (buffer);
        buffer.add (peer_guid);
        buffer.add (low_guid);
        buffer.add (high_guid);
        buffer.add (expansion);
        buffer.add (level);
        buffer.add (low_time);
        buffer.add (expected_hash.length);
        buffer.add (expected_hash);
        buffer.add (seq);
    }

    public Object clone () throws CloneNotSupportedException {
	FetchMerkleTreeNodeReq result = (FetchMerkleTreeNodeReq) super.clone ();
        result.peer_guid = peer_guid;
        result.low_guid = low_guid;
        result.high_guid = high_guid;
        result.expansion = expansion;
        result.level = level;
        result.low_time = low_time;
        result.expected_hash = expected_hash;
        result.seq = seq;
        return result;
    }

    public String toString () {
	StringBuffer result = new StringBuffer (100);
	result.append ("(FetchMerkleTreeNodeReq super=");
	result.append (super.toString ());
	result.append (" peer_guid="); 
	result.append (GuidTools.guid_to_string (peer_guid));
	result.append (" low_guid="); 
	result.append (GuidTools.guid_to_string (low_guid));
	result.append (" high_guid="); 
	result.append (GuidTools.guid_to_string (high_guid));
	result.append (" expansion="); result.append (expansion);
	result.append (" level="); result.append (level);
	result.append (" low_time="); 
        result.append (Long.toHexString (low_time));
	result.append (" expected_hash="); 
        result.append (ostore.util.ByteUtils.print_bytes (expected_hash));
	result.append (" seq="); result.append (seq);
	result.append (")");
	return result.toString ();
    }
}

