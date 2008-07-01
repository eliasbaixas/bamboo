/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.api;
import java.math.BigInteger;
import ostore.util.QuickSerializable;
import ostore.util.NodeId;
import seda.sandStorm.api.QueueElementIF;
import bamboo.util.GuidTools;

/**
 * Called on intermediate nodes along the routing path.  See the 
 * comments for {@link BambooRouteInit} for more information.
 * 
 * @author Sean C. Rhea
 * @version $Id: BambooRouteUpcall.java,v 1.2 2003/12/06 22:36:55 srhea Exp $
 */
public class BambooRouteUpcall implements QueueElementIF {

    public BigInteger src, dest;

    // The last person this message came through
    public NodeId immediate_src;

    public long app_id;
    public boolean iter;

    public int tries;
    public long wait_ms;
    public long est_rtt_ms;

    public QuickSerializable payload;

    public BambooRouteUpcall (BigInteger s, BigInteger d, NodeId i, long a, 
	    boolean it, int att, long w, long e, QuickSerializable p) {
	src = s; dest = d; immediate_src = i; app_id = a; 
	iter = it; tries = att; wait_ms = w; est_rtt_ms = e; payload = p;
    }

    public String toString () {
	return "(BambooRouteUpcall src=" + GuidTools.guid_to_string (src) +
	    " dest=" + GuidTools.guid_to_string (dest) + " immediate_src=" +
	    immediate_src + " app_id=" + Long.toHexString (app_id) + 
	    " iter=" + iter + " tries=" + tries + " wait_ms=" + wait_ms + 
            " est_rtt_ms=" + est_rtt_ms + " payload=" + " payload=" + 
            payload + ")";
    }
}

