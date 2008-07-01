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
 * IterRouteReq.
 *
 * @author  Sean C. Rhea
 * @version $Id: IterRouteReq.java,v 1.5 2003/10/05 18:22:11 srhea Exp $
 */
public class IterRouteReq extends NetworkMessage {

    public BigInteger dest;
    public BigInteger src;
    public long app_id;
    public boolean intermediate_upcall;
    public QuickSerializable payload;

    public IterRouteReq (IterRouteResp resp) {
	super (resp.next_hop.node_id, false);  
	src = resp.src; 
	dest = resp.dest;  
	app_id = resp.app_id;  
	payload = resp.payload;  
	intermediate_upcall = resp.intermediate_upcall;
    }

    public IterRouteReq (NodeId n, BigInteger s, BigInteger d, long a, 
	    boolean u, QuickSerializable p) {
	super (n, false);  
	src = s; 
	dest = d;  
	app_id = a;  
	payload = p;  
	intermediate_upcall = u;
    }

    public IterRouteReq (InputBuffer buffer) throws QSException {
	super (buffer);
	src = buffer.nextBigInteger ();
	dest = buffer.nextBigInteger ();
	app_id = buffer.nextLong ();
	intermediate_upcall = buffer.nextBoolean ();
	payload = buffer.nextObject ();
    }

    public void serialize (OutputBuffer buffer) {
	super.serialize (buffer);
        buffer.add (src);
        buffer.add (dest);
        buffer.add (app_id);
        buffer.add (intermediate_upcall);
        buffer.add (payload);
    }

    public Object clone () throws CloneNotSupportedException {
	IterRouteReq result = (IterRouteReq) super.clone ();
	result.src = src;
	result.dest = dest;
	result.app_id = app_id;
	result.intermediate_upcall = intermediate_upcall;
	result.payload = payload;
	return result;
    }

    public String toString () {
	return "(IterRouteReq " + super.toString() +
	    " src=" + bamboo.util.GuidTools.guid_to_string(src) + 
	    " dest=" + bamboo.util.GuidTools.guid_to_string(dest) + 
	    " app_id=" + app_id + 
	    " intermediate_upcall=" + intermediate_upcall +
	    " payload=" + payload + ")";
    }
}

