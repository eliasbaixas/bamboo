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

/**
 * FetchKeysReq.
 *
 * @author  Sean C. Rhea
 * @version $Id: FetchKeysReq.java,v 1.4 2004/04/20 19:23:17 srhea Exp $
 */
public class FetchKeysReq extends NetworkMessage {

    public BigInteger low_guid;
    public BigInteger high_guid;
    public long low_time;
    public long high_time;

    /**
     * To pair it up with its response.
     */
    public long seq;

    public FetchKeysReq (NodeId dest, BigInteger lg, BigInteger hg,
	    long lt, long ht, long s) {
	super (dest, false);
        low_guid = lg; high_guid = hg; low_time = lt; high_time = ht; seq = s;
    }

    public FetchKeysReq (InputBuffer buffer) throws QSException {
	super (buffer);
	low_guid = buffer.nextBigInteger ();
	high_guid = buffer.nextBigInteger ();
	low_time = buffer.nextLong ();
	high_time = buffer.nextLong ();
	seq = buffer.nextLong ();
    }

    public void serialize (OutputBuffer buffer) {
	super.serialize (buffer);
        buffer.add (low_guid);
        buffer.add (high_guid);
        buffer.add (low_time);
        buffer.add (high_time);
        buffer.add (seq);
    }

    public Object clone () throws CloneNotSupportedException {
	FetchKeysReq result = (FetchKeysReq) super.clone ();
        result.low_guid = low_guid;
        result.high_guid = high_guid;
        result.low_time = low_time;
        result.high_time = high_time;
        result.seq = seq;
        return result;
    }

    public String toString () {
	StringBuffer result = new StringBuffer (100);
	result.append ("(FetchKeysReq super=");
	result.append (super.toString ());
	result.append (" low_guid=");
	result.append (GuidTools.guid_to_string (low_guid));
	result.append (" high_guid=");
	result.append (GuidTools.guid_to_string (high_guid));
	result.append (" low_time=");
        result.append (Long.toHexString (low_time));
	result.append (" high_time=");
        result.append (Long.toHexString (high_time));
	result.append (" seq="); result.append (seq);
	result.append (")");
	return result.toString ();
    }
}

