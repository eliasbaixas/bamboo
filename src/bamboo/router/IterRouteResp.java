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

/**
 * IterRouteResp.
 *
 * @author  Sean C. Rhea
 * @version $Id: IterRouteResp.java,v 1.6 2003/10/05 18:22:11 srhea Exp $
 */
public class IterRouteResp extends NetworkMessage {

    public BigInteger dest;
    public BigInteger src;
    public long app_id;
    public boolean intermediate_upcall;
    public QuickSerializable payload;
    public NeighborInfo next_hop;

    public IterRouteResp (NodeId n, BigInteger s, BigInteger d, long a, 
	    boolean u, QuickSerializable p, NeighborInfo nh) {
	super (n, false);  
	src = s; 
	dest = d;  
	app_id = a;  
	payload = p;  
	intermediate_upcall = u;
	next_hop = nh;
    }

    public IterRouteResp (InputBuffer buffer) throws QSException {
	super (buffer);
	src = buffer.nextBigInteger ();
	dest = buffer.nextBigInteger ();
	app_id = buffer.nextLong ();
	intermediate_upcall = buffer.nextBoolean ();
	payload = buffer.nextObject ();
	next_hop = new NeighborInfo (buffer);
    }

    public void serialize (OutputBuffer buffer) {
	super.serialize (buffer);
        buffer.add (src);
        buffer.add (dest);
        buffer.add (app_id);
        buffer.add (intermediate_upcall);
        buffer.add (payload);
        next_hop.serialize (buffer);
    }

    public Object clone () throws CloneNotSupportedException {
	IterRouteResp result = (IterRouteResp) super.clone ();
	result.src = src;
	result.dest = dest;
	result.app_id = app_id;
	result.intermediate_upcall = intermediate_upcall;
	result.payload = payload;
	result.next_hop = next_hop;
	return result;
    }

    public String toString () {
	return "(IterRouteResp " + super.toString() +
	    " src=" + bamboo.util.GuidTools.guid_to_string(src) + 
	    " dest=" + bamboo.util.GuidTools.guid_to_string(dest) + 
	    " app_id=" + app_id + 
	    " intermediate_upcall=" + intermediate_upcall +
	    " payload=" + payload + 
	    " next_hop=" + next_hop + ")";
    }
}

