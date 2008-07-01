/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.api;
import java.math.BigInteger;
import ostore.util.QuickSerializable;
import seda.sandStorm.api.QueueElementIF;
import bamboo.util.GuidTools;

/**
 * Initiate a routing operation to <code>dest</code>.  Place the message to
 * be sent in <code>payload</code>.  To receive the message, the node
 * responsible for <code>dest</code> must have registered an application
 * (see <code>BambooRouterAppRegReq</code>) with identifier
 * <code>app_id</code>.  If <code>intermediate_upcall</code> is true, a
 * <code>BambooRouteUpcall</code> event will be sent to the application on
 * each intermediate node in the path.  To continue the routing operation,
 * that node must send a <code>BambooRouteContinue</code> event.  Finally, a
 * <code>BambooRouteDeliver</code> event will be sent to the application
 * once the message reaches the node responsible for <code>app_id</code>.
 * 
 * @author Sean C. Rhea
 * @version $Id: BambooRouteInit.java,v 1.1 2003/10/05 19:02:02 srhea Exp $
 */
public class BambooRouteInit implements QueueElementIF {

    public BigInteger dest;
    public long app_id;
    public boolean intermediate_upcall;
    public boolean iter;
    public QuickSerializable payload;

    public BambooRouteInit (
	    BigInteger d, long a, boolean u, boolean i, QuickSerializable p) {
	dest = d; app_id = a; intermediate_upcall = u; iter = i; payload = p;
    }

    public String toString () {
	return "(BambooRouteInit dest=" + GuidTools.guid_to_string (dest) + 
	    " app_id=" + Long.toHexString (app_id) + " intermediate_upcall=" + 
	    intermediate_upcall + " iter=" + iter + " payload=" + 
	    payload + ")";
    }
}

