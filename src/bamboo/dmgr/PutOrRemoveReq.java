/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dmgr;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SinkIF;
import bamboo.util.GuidTools;
import java.net.InetAddress;

/**
 * Ask the DataManager to add a new put or remove.
 *
 * @author Sean C. Rhea
 * @version $Id: PutOrRemoveReq.java,v 1.8 2005/05/12 00:08:19 srhea Exp $
 */
public class PutOrRemoveReq implements QueueElementIF {

    public long time_usec;
    public int ttl_sec;
    public BigInteger guid;
    public ByteBuffer value;         // only for puts
    public byte [] secret_hash;
    public byte [] value_hash;       // only for removes
    public boolean put;              // whether this is a put or a remove
    public InetAddress client_id;
    public SinkIF completion_queue;  // for the resulting ack
    public Object user_data;         // for the resulting ack

    public PutOrRemoveReq (long t, int tt, BigInteger g, ByteBuffer v,
	                   byte [] sh, byte [] vh, boolean p, InetAddress c, 
                           SinkIF q, Object u) {
	time_usec = t;
	ttl_sec = tt;
	guid = g;
	value = v;
        secret_hash = sh;
        value_hash = vh;
	put = p;
        client_id = c;
	completion_queue = q;
        user_data = u;
    }

    public String toString () {
	return "(PutOrRemoveReq time_usec=" + time_usec + " guid=" +
	    GuidTools.guid_to_string (guid) + " value=<> put=" + put +
	    " user_data=" + user_data + ")";
    }
}

