/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.vis;

import bamboo.router.NeighborInfo;
import java.math.BigInteger;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;
import bamboo.util.GuidTools;

public class ExtendedNeighborInfo extends NeighborInfo {
    //For clarification:
    //NodeId node_id = addr
    //BigInteger guid = id

    public long rtt_ms;

    public ExtendedNeighborInfo (NeighborInfo n) {
        super (n.node_id, n.guid);
    }

    public ExtendedNeighborInfo (NodeId n, BigInteger g) {
	super (n, g);
    }

    public ExtendedNeighborInfo (NodeId a, BigInteger i, long r) {
        super (a, i);
        rtt_ms = r;
    }   
}
