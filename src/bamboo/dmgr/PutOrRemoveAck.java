/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dmgr;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.LinkedList;
import ostore.network.NetworkMessage;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;

/**
 * PutOrRemoveAck.
 *
 * @author  Sean C. Rhea
 * @version $Id: PutOrRemoveAck.java,v 1.4 2004/08/04 19:23:27 srhea Exp $
 */
public class PutOrRemoveAck extends NetworkMessage {

    public long seq;

    public PutOrRemoveAck (NodeId dest, long s) {
	super (dest, false);  seq = s;
    }

    public PutOrRemoveAck (InputBuffer buffer) throws QSException {
	super (buffer);
	seq = buffer.nextLong ();
    }

    public void serialize (OutputBuffer buffer) {
	super.serialize (buffer);
        buffer.add (seq);
    }

    public Object clone () throws CloneNotSupportedException {
	PutOrRemoveAck result = (PutOrRemoveAck) super.clone ();
	result.seq = seq;
	return result;
    }

    public String toString () {
	StringBuffer result = new StringBuffer (100);
	result.append ("(PutOrRemoveAck super=");
	result.append (super.toString ());
	result.append (" seq="); result.append (seq);
	result.append (")");
	return result.toString ();
    }
}

