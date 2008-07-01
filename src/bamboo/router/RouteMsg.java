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
import ostore.util.QuickSerializable;
import static bamboo.lss.UdpCC.ByteCount;

/**
 * RouteMsg.
 *
 * @author  Sean C. Rhea
 * @version $Id: RouteMsg.java,v 1.9 2005/08/16 05:11:44 srhea Exp $
 */
public class RouteMsg extends NetworkMessage implements ByteCount {

    public BigInteger dest;
    public BigInteger src;
    public long app_id;
    public boolean intermediate_upcall;
    public BigInteger peer_guid;
    public QuickSerializable payload;

    public String byteCountKey() { 
        return ((ByteCount) payload).byteCountKey(); 
    }

    public boolean recordByteCount() { 
        if (payload instanceof ByteCount) 
            return ((ByteCount) payload).recordByteCount(); 
        else 
            return false;
    }

    public RouteMsg (NodeId n, BigInteger s, BigInteger d, long a, 
	    boolean u, BigInteger g, QuickSerializable p) {
	super (n, false);  
	src = s; 
	dest = d;  
	app_id = a;  
	payload = p;  
        peer_guid = g;
	intermediate_upcall = u;
    }

    public RouteMsg (InputBuffer buffer) throws QSException {
	super (buffer);
	src = buffer.nextBigInteger ();
	dest = buffer.nextBigInteger ();
	app_id = buffer.nextLong ();
	intermediate_upcall = buffer.nextBoolean ();
	peer_guid = buffer.nextBigInteger ();
	payload = buffer.nextObject ();
    }

    public void serialize (OutputBuffer buffer) {
	super.serialize (buffer);
        buffer.add (src);
        buffer.add (dest);
        buffer.add (app_id);
        buffer.add (intermediate_upcall);
        buffer.add (peer_guid);
        buffer.add (payload);
    }

    public Object clone () throws CloneNotSupportedException {
	RouteMsg result = (RouteMsg) super.clone ();
	result.src = src;
	result.dest = dest;
	result.app_id = app_id;
	result.intermediate_upcall = intermediate_upcall;
	result.payload = payload;
        result.peer_guid = peer_guid;
	return result;
    }

    public String toString () {
	return "(RouteMsg " + super.toString() +
	    " src=" + bamboo.util.GuidTools.guid_to_string(src) + 
	    " dest=" + bamboo.util.GuidTools.guid_to_string(dest) + 
	    " app_id=" + Long.toHexString (app_id) + 
	    " intermediate_upcall=" + intermediate_upcall +
	    " peer_guid=" + bamboo.util.GuidTools.guid_to_string (peer_guid) + 
            " payload=" + payload + ")";
    }
}

