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
 * Sent when a routing operation reaches the node responsible for
 * <code>dest</code>.  See the comments for {@link BambooRouteInit} 
 * for more information.
 * 
 * @author Sean C. Rhea
 * @version $Id: BambooRouteDeliver.java,v 1.2 2003/12/06 22:36:55 srhea Exp $
 */
public class BambooRouteDeliver implements QueueElementIF {

    public BigInteger src, dest;

    // The last person this message came through
    public NodeId immediate_src;

    public long app_id;

    public int tries;
    public long wait_ms;
    public long est_rtt_ms;

    public QuickSerializable payload;

    public BambooRouteDeliver (BigInteger s, BigInteger d, NodeId i, 
            long a, int att, long w, long e, QuickSerializable p) {
	src = s; dest = d; immediate_src = i; app_id = a;
        tries = att; wait_ms = w; est_rtt_ms = e; payload = p;
    }

    public String toString () {
	return "(BambooRouteDeliver src=" + GuidTools.guid_to_string (src) +
	    " dest=" + GuidTools.guid_to_string (dest) + " immediate_src=" +
	    immediate_src + " app_id=" + Long.toHexString (app_id) + 
            " tries=" + tries + " wait_ms=" + wait_ms + " est_rtt_ms=" +
            est_rtt_ms + " payload=" + payload + ")";
    }
}

