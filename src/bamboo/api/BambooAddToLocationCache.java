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
 * @version $Id: BambooAddToLocationCache.java,v 1.1 2003/12/29 18:10:43 srhea Exp $
 */
public class BambooAddToLocationCache implements QueueElementIF {

    public NodeId node_id;
    public BigInteger guid;

    public BambooAddToLocationCache (NodeId n, BigInteger g) {
        guid = g; node_id = n;
    }

    public String toString () {
	return "(BambooAddToLocationCache guid=" 
            + GuidTools.guid_to_string (guid) + " node_id=" + node_id + ")";
    }
}
