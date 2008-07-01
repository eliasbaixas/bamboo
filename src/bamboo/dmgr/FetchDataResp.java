/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dmgr;
import java.nio.ByteBuffer;
import ostore.network.NetworkMessage;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import bamboo.db.StorageManager;

/**
 * FetchDataResp, assuming it fits in a packet for now.
 *
 * @author  Sean C. Rhea
 * @version $Id: FetchDataResp.java,v 1.4 2004/04/22 21:17:19 srhea Exp $
 */
public class FetchDataResp extends NetworkMessage {

    public StorageManager.Key key;
    public ByteBuffer data;

    public FetchDataResp (NodeId dest, StorageManager.Key k, ByteBuffer d) {
	super (dest, false);
        key = k; data = d;
    }

    public FetchDataResp (InputBuffer buffer) throws QSException {
	super (buffer);
        key = new StorageManager.Key (buffer);
        int len = buffer.nextInt ();
        if (len > 0) {
            byte [] dbuf = new byte [len];
            buffer.nextBytes (dbuf, 0, len);
            data = ByteBuffer.wrap (dbuf);
        }
    }

    public void serialize (OutputBuffer buffer) {
	super.serialize (buffer);
        key.serialize (buffer);
        if (data == null)
            buffer.add (0);
        else {
            buffer.add (data.limit ());
            buffer.add (data.array (), data.arrayOffset (), data.limit ());
        }
    }

    public Object clone () throws CloneNotSupportedException {
	FetchDataResp result = (FetchDataResp) super.clone ();
        result.key = key;
        result.data = data;
        return result;
    }

    public String toString () {
	StringBuffer result = new StringBuffer (100);
	result.append ("(FetchDataResp super=");
	result.append (super.toString ());
	result.append (" key=");
        result.append (key);
        result.append (data == null ? " data=null" : " data=<...>");
	result.append (")");
	return result.toString ();
    }
}

