/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.softscribe;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.LinkedList;
import ostore.util.AssertionViolatedException;
import ostore.util.CountBuffer;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;
import bamboo.util.GuidTools;
import ostore.network.NetworkMessage;
import seda.sandStorm.api.QueueElementIF;

/**
 * MCastJoin
 *
 * To join a multicast group, a stage sends an MCastJoin with inbound=false and
 * join=true.  To remove itself from a multicast group, a stage sends an
 * MCastJoin with inbound=false and join=false.
 *
 * @author  David Oppenheimer
 * @version $Id: MCastJoin.java,v 1.3 2004/08/04 19:23:27 srhea Exp $ */

public class MCastJoin implements QueueElementIF {
    public boolean inbound;
    public BigInteger groupguid; // identifies the multicast group
    public boolean join;      // whether to join

    public MCastJoin (boolean inbound, BigInteger groupguid, boolean join) {
	this.inbound = inbound;
	this.groupguid = groupguid;
	this.join = join;
    }

    public MCastJoin (MCastJoin other) {
	inbound = other.inbound;
	groupguid = other.groupguid;
	join = other.join;
    }

    public MCastJoin (InputBuffer buffer) throws QSException {
        inbound = buffer.nextBoolean();
	groupguid = buffer.nextBigInteger();
	join = buffer.nextBoolean();
    }

    public void serialize (OutputBuffer buffer) {
        buffer.add (inbound);
	buffer.add(groupguid);
	buffer.add(join);
    }

    public String toString () {
	return "(MCastJoin  groupguid=" + GuidTools.guid_to_string (groupguid) +
	    " inbound=" + inbound +
	    " join=" + join +
	    ")";
    }
}

