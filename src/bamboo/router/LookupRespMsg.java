/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.router;
import bamboo.util.GuidTools;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.LinkedList;
import ostore.network.NetworkMessage;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;

/**
 * LookupRespMsg.
 *
 * @author  Sean C. Rhea
 * @version $Id: LookupRespMsg.java,v 1.3 2004/03/23 23:42:19 srhea Exp $
 */
public class LookupRespMsg extends NetworkMessage {

    public BigInteger lookup_id;
    public BigInteger owner_id;

    public LookupRespMsg (NodeId dest, BigInteger l, BigInteger o) {
	super (dest, false); lookup_id = l; owner_id = o;
    }

    public LookupRespMsg (InputBuffer buffer) throws QSException {
	super (buffer);
        lookup_id = buffer.nextBigInteger ();
        owner_id = buffer.nextBigInteger ();
    }

    public void serialize (OutputBuffer buffer) {
	super.serialize (buffer);
        buffer.add (lookup_id);
        buffer.add (owner_id);
    }

    public Object clone () throws CloneNotSupportedException {
        LookupRespMsg result = (LookupRespMsg) super.clone ();
        result.lookup_id = lookup_id;
        result.owner_id = owner_id;
        return result;
    }

    public String toString () {
        return "(LookupRespMsg super=" + super.toString () + " lookup_id=0x" 
            + GuidTools.guid_to_string (lookup_id) + " owner_id=0x" 
            + GuidTools.guid_to_string (owner_id) + ")";
    }
}

