/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dmgr;
import ostore.network.NetworkMessage;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import bamboo.db.StorageManager;

/**
 * FetchDataReq, assuming it fits in a packet for now.
 *
 * @author  Sean C. Rhea
 * @version $Id: FetchDataReq.java,v 1.4 2004/04/22 21:17:19 srhea Exp $
 */
public class FetchDataReq extends NetworkMessage {

    public StorageManager.Key key;

    public FetchDataReq (NodeId dest, StorageManager.Key k) {
	super (dest, false);
        key = k;
    }

    public FetchDataReq (InputBuffer buffer) throws QSException {
	super (buffer);
        key = new StorageManager.Key (buffer);
    }

    public void serialize (OutputBuffer buffer) {
	super.serialize (buffer);
        key.serialize (buffer);
    }

    public Object clone () throws CloneNotSupportedException {
	FetchDataReq result = (FetchDataReq) super.clone ();
        result.key = key;
        return result;
    }

    public String toString () {
	StringBuffer result = new StringBuffer (100);
	result.append ("(FetchDataReq super=");
	result.append (super.toString ());
	result.append (" key=");
        result.append (key);
	result.append (")");
	return result.toString ();
    }
}

