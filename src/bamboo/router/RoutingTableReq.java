/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.router;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.LinkedList;
import ostore.network.NetworkMessage;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import bamboo.util.GuidTools;

/**
 * RoutingTableReq.
 *
 * @author  Sean C. Rhea
 * @version $Id: RoutingTableReq.java,v 1.5 2003/10/05 18:22:11 srhea Exp $
 */
public class RoutingTableReq extends NetworkMessage {

    public BigInteger guid;
    public int level;

    public RoutingTableReq (NodeId dest, BigInteger g, int l) {
	super (dest, false);  guid = g;  level = l;
    }

    public RoutingTableReq (InputBuffer buffer) throws QSException {
	super (buffer);
	guid = buffer.nextBigInteger ();
	level = buffer.nextInt ();
    }

    public void serialize (OutputBuffer buffer) {
	super.serialize (buffer);
        buffer.add (guid);
        buffer.add (level);
    }

    public Object clone () throws CloneNotSupportedException {
	RoutingTableReq result = (RoutingTableReq) super.clone ();
	result.guid = guid;
	result.level = level;
	return result;
    }

    public String toString () {
	return "(RoutingTableReq super=" + super.toString () +
	    " guid=0x" + GuidTools.guid_to_string (guid) + 
	    " level=" + level + ")";
    }
}

