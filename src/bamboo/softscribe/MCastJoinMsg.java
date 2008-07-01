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
 * MCastJoinMsg
 *
 * A Bamboo payload indicating that the sending node considers itself to be
 *    a member of the indicated group. This message serves both as the initial
 *    indication that the sender has joined, and as a periodic heartbeat.
 *
 * @author  David Oppenheimer
 * @version $Id: MCastJoinMsg.java,v 1.2 2004/08/04 19:23:27 srhea Exp $
 */
public class MCastJoinMsg implements QuickSerializable, QueueElementIF {

    public NodeId srcid;
    public BigInteger dstguid; // identifies the group

    public MCastJoinMsg (NodeId srcid, BigInteger dstguid) {
	this.srcid = srcid;
	this.dstguid = dstguid;
    }

    public MCastJoinMsg (MCastJoinMsg other) {
	srcid = other.srcid;
	dstguid = other.dstguid;
    }

    public MCastJoinMsg (InputBuffer buffer) throws QSException {
	srcid = (NodeId)buffer.nextObject ();
	dstguid = buffer.nextBigInteger ();
    }

    public void serialize (OutputBuffer buffer) {
	buffer.add (srcid);
	buffer.add (dstguid);
    }

    public String toString () {
	return "(MCastJoinMsg srcid=" + srcid + 
	    " dstguid=" + GuidTools.guid_to_string (dstguid) + 
	    ")";
    }
}

