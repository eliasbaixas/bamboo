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
 * MCastDownMsg
 *
 * A NetworkMessage to be sent down the tree. At each subcriber, an inbound
 *   MCastDown is generated and sent to the classifier, and the MCastDownMsg is
 *   re-sent to the subscriber's children (in the subscription tree, not the
 *   overlay tree).
 *
 * @author  David Oppenheimer
 * @version $Id: MCastDownMsg.java,v 1.3 2004/08/04 19:23:27 srhea Exp $ */
public class MCastDownMsg extends NetworkMessage {

    public BigInteger srcguid; // the guid of the root, identifying the group
    public NodeId srcid;
    public NodeId dstid;
    public QuickSerializable o;

    public MCastDownMsg (BigInteger srcguid, NodeId srcid, NodeId dstid, 
			 QuickSerializable o) {
	super(dstid, false /* outbound */);
	this.srcguid = srcguid;
	this.srcid = srcid;
	this.dstid = dstid;
	this.o = o;
    }

    public MCastDownMsg (InputBuffer buffer) throws QSException {
	super(buffer);
	srcguid = buffer.nextBigInteger ();
	srcid = (NodeId)buffer.nextObject ();
	dstid = (NodeId)buffer.nextObject ();
	o = buffer.nextObject ();
    }

    public void serialize (OutputBuffer buffer) {
	super.serialize(buffer);
	buffer.add (srcguid);
	buffer.add (srcid);
	buffer.add (dstid);
	buffer.add (o);
    }

    public String toString () {
	return "(MCastDownMsg  srcguid=" + GuidTools.guid_to_string (srcguid) +
	    " srcid=" + srcid + 
	    " dstid=" + dstid +
	    " o=" + o +
	    ")";
    }
}

