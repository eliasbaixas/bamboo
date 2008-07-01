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
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;
import bamboo.util.GuidTools;

/**
 * LookupReqPayload.
 *
 * @author  Sean C. Rhea
 * @version $Id: LookupReqPayload.java,v 1.1 2003/12/20 21:48:12 srhea Exp $
 */
public class LookupReqPayload implements QuickSerializable {

    public NodeId rtn_addr;

    public LookupReqPayload (NodeId s) { rtn_addr = s; }

    public LookupReqPayload (InputBuffer buffer) throws QSException {
        rtn_addr = new NodeId (buffer);
    }

    public void serialize (OutputBuffer buffer) {
	rtn_addr.serialize (buffer);
    }

    public String toString () {
	return "(LookupReqPayload super=" + super.toString () +
	    " rtn_addr=" + rtn_addr + ")";
    }
}

