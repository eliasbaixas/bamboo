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
import ostore.util.SHA1Hash;
import bamboo.util.GuidTools;
import bamboo.db.StorageManager;

/**
 * FetchKeysResp.
 *
 * @author  Sean C. Rhea
 * @version $Id: FetchKeysResp.java,v 1.5 2004/05/18 19:11:32 srhea Exp $
 */
public class FetchKeysResp extends NetworkMessage {

    public LinkedList keys;

    /**
     * To pair it up with its request.
     */
    public long seq;

    public FetchKeysResp (NodeId dest, LinkedList k, long s) {
	super (dest, false);
        keys = k; seq = s;
    }

    public FetchKeysResp (InputBuffer buffer) throws QSException {
	super (buffer);
	int cnt = buffer.nextInt ();
        if (cnt > 0) {
            keys = new LinkedList ();
            while (cnt-- > 0) {
                StorageManager.Key k = new StorageManager.Key (buffer);
                keys.addLast (k);
            }
        }
	seq = buffer.nextLong ();
    }

    public void serialize (OutputBuffer buffer) {
	super.serialize (buffer);
        if (keys == null)
            buffer.add (0);
        else {
            buffer.add (keys.size ());
            Iterator i = keys.iterator ();
            while (i.hasNext ()) {
                StorageManager.Key k = (StorageManager.Key) i.next ();
                k.serialize (buffer);
            }
        }
        buffer.add (seq);
    }

    public Object clone () throws CloneNotSupportedException {
        FetchKeysResp result = (FetchKeysResp) super.clone();
        if (keys != null) {
            result.keys = new LinkedList ();
            Iterator i = keys.iterator ();
            while (i.hasNext())
                result.keys.addLast (i.next ());
        }
        result.seq = seq;
        return result;
    }

    public String toString () {
	StringBuffer result = new StringBuffer (100+
                ((keys == null) ? 0 : 100*keys.size ()));
	result.append ("(FetchKeysResp super=");
        result.append (super.toString ());
	result.append (" keys=(");
        if (keys != null) {
            Iterator i = keys.iterator ();
            while (i.hasNext ()) {
                StorageManager.Key k = (StorageManager.Key) i.next ();
                result.append (k);
                if (i.hasNext ()) result.append (" ");
            }
        }
	result.append (") seq="); result.append (seq);
	result.append (")");
	return result.toString ();
    }
}

