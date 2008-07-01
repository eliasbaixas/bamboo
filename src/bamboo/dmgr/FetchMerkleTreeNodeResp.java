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
 * FetchMerkleTreeNodeResp.  
 *
 * <p>Wire format:
 * <br>
 * <center>
 * <table border="1">
 * <tr><td>Bytes</td><td>Data</td></tr>
 * <tr><td> 0-19 </td><td>IP header</td></tr>
 * <tr><td>20-27</td><td>UdpCC ACK</td></tr>
 * <tr><td>28-35</td><td>UdpCC SEQ</td></tr>
 * <tr><td>36-55</td><td>hash</td></tr>
 * <tr><td> 56  </td><td>leaf</td></tr>
 * <tr><td>57-64</td><td>seq</td></tr>
 * <tr><td>65-68</td><td>number of child hashes</td></tr>
 * <tr><td> 69- </td><td>child hashes, 20 bytes apiece</td></tr>
 * </table>
 * </center>
 * <br>
 * As such, the first 69 bytes of the message are header.  Assuming an MTU of
 * 500 bytes, we can thus pack (500-69)/20 = 21 child pointers into a single
 * packet response.  Assuming an MTU of 1500 bytes, we can pack 71 in.  So a
 * "good" expansion factor for the tree would be 2^6=64.
 *
 * @author  Sean C. Rhea
 * @version $Id: FetchMerkleTreeNodeResp.java,v 1.5 2004/02/10 21:24:04 srhea Exp $
 */
public class FetchMerkleTreeNodeResp extends NetworkMessage {

    /**
     * Our hash for this node.
     */
    public byte [] hash;

    /**
     * Whether this node was a leaf.
     */
    public boolean leaf;

    /**
     * The children of the node, if the hashes didn't match and the node was
     * not a leaf.  These are always hashes of the same length as
     * <code>hash</code>, and there are always 2^<code>expansion</code> of
     * them in left-to-right, smallest-to-largest order.
     */
    public LinkedList children;

    /**
     * To pair it up with its request.
     */
    public long seq;

    public FetchMerkleTreeNodeResp (NodeId dest, byte [] h, boolean l, 
            LinkedList c, long s) {
	super (dest, false);
        hash = h; leaf = l; children = c; seq = s;
    }

    public FetchMerkleTreeNodeResp (InputBuffer buffer) throws QSException {
	super (buffer);
	int len = buffer.nextInt ();
        hash = new byte [len];
        buffer.nextBytes (hash, 0, len);
        leaf = buffer.nextBoolean ();
	seq = buffer.nextLong ();
	int cnt = buffer.nextInt ();
        if (cnt > 0) {
            children = new LinkedList ();
            while (cnt-- > 0) {
                byte [] child = new byte [len];
                buffer.nextBytes (child, 0, len);
                children.addLast (child);
            }
        }
    }

    public void serialize (OutputBuffer buffer) {
	super.serialize (buffer);
        buffer.add (hash.length);
        buffer.add (hash);
        buffer.add (leaf);
        buffer.add (seq);
        if (children == null) 
            buffer.add (0);
        else {
            buffer.add (children.size ());
            Iterator i = children.iterator ();
            while (i.hasNext ()) {
                byte [] child = (byte []) i.next ();
                buffer.add (child, 0, hash.length);
            }
        }
    }

    public Object clone () throws CloneNotSupportedException {
        FetchMerkleTreeNodeResp result = 
            (FetchMerkleTreeNodeResp) super.clone ();
        result.hash = hash; 
        result.leaf = leaf;
        result.seq = seq;
        result.children = children;
        return result;
    }

    public String toString () {
	StringBuffer result = new StringBuffer (100+
                ((children == null) ? 0 : 40*children.size ()));
	result.append ("(FetchMerkleTreeNodeResp super=");
	result.append (super.toString ());
	result.append (" hash="); 
        result.append (ostore.util.ByteUtils.print_bytes (hash));
	result.append (" leaf="); result.append (leaf);
	result.append (" children=(");
        if (children != null) {
            Iterator i = children.iterator ();
            while (i.hasNext ()) {
                byte [] child = (byte []) i.next ();
                result.append ("<");
                result.append (ostore.util.ByteUtils.print_bytes (child));
                result.append (">");
                if (i.hasNext ()) result.append (" ");
            }
        }
	result.append (") seq="); result.append (seq);
	result.append (")");
	return result.toString ();
    }
}

