/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.api;
import java.math.BigInteger;
import ostore.util.NodeId;
import ostore.util.QuickSerializable;
import seda.sandStorm.api.QueueElementIF;
import bamboo.util.GuidTools;

/**
 * Coninue a routing operation after an upcall.  See the comments for
 * {@link BambooRouteInit} for more information.
 * 
 * @author Sean C. Rhea
 * @version $Id: BambooRouteContinue.java,v 1.1 2003/10/05 19:02:02 srhea Exp $
 */
public class BambooRouteContinue implements QueueElementIF {

    public BigInteger src, dest;

    // The last person this message came through
    public NodeId immediate_src;

    public long app_id;
    public boolean intermediate_upcall;
    public boolean iter;
    public QuickSerializable payload;

    public BambooRouteContinue (BambooRouteUpcall upcall, QuickSerializable p) {
	src = upcall.src;  
	dest = upcall.dest;  
	immediate_src = upcall.immediate_src;
	app_id = upcall.app_id;  
	intermediate_upcall = true;
	iter = upcall.iter;
	payload = p;
    }

    public BambooRouteContinue (BigInteger s, BigInteger d, NodeId i, 
	    long a, boolean u, boolean it, QuickSerializable p) {
	src = s; dest = d; immediate_src = i; app_id = a;  
	intermediate_upcall = u; iter = it; payload = p;
    }

    public String toString () {
	return "(BambooRouteContinue src=" + GuidTools.guid_to_string (src) + 
	    " dest=" + GuidTools.guid_to_string (dest) + " immediate_src=" +
	    immediate_src + " app_id=" + Long.toHexString (app_id) + 
	    " intermediate_upcall=" + intermediate_upcall + " iter=" + iter + 
	    " payload=" + payload + ")";
    }
}

