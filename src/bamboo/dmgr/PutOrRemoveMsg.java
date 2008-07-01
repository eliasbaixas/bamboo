/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dmgr;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import ostore.network.NetworkMessage;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import bamboo.util.GuidTools;
import java.net.InetAddress;
import java.net.*;
import static bamboo.db.StorageManager.ZERO_HASH;

/**
 * PutOrRemoveMsg.
 *
 * @author  Sean C. Rhea
 * @version $Id: PutOrRemoveMsg.java,v 1.9 2005/05/12 00:08:19 srhea Exp $
 */
public class PutOrRemoveMsg extends NetworkMessage {

    public static final int MAX_SIZE = 16384;

    public long time_usec;
    public int ttl_sec;
    public BigInteger guid;
    public ByteBuffer value;
    public boolean put;
    public InetAddress client_id;
    public long seq;
    public byte [] value_hash;
    public byte [] secret_hash;

    public PutOrRemoveMsg (NodeId dest, long t, int tt, BigInteger g,
			   ByteBuffer v, boolean p, InetAddress c, long s,
                           byte [] vh, byte [] sh) {
	super (dest, false);
	time_usec = t; ttl_sec = tt; guid = g; value = v; put = p; seq = s;
        client_id = c; value_hash = vh; secret_hash = sh;
    }

    public PutOrRemoveMsg (InputBuffer buffer) throws QSException {
	super (buffer);
	time_usec = buffer.nextLong ();
	ttl_sec = buffer.nextInt ();
	guid = buffer.nextBigInteger ();
	int len = buffer.nextInt ();
	if (len > MAX_SIZE) throw new QSException ("len=" + len);
	byte [] tmp = new byte [len];
	buffer.nextBytes (tmp, 0, len);
	value = ByteBuffer.wrap (tmp);
	put = buffer.nextBoolean ();
        len = buffer.nextInt ();
        byte [] client_bytes = new byte [len];
        buffer.nextBytes (client_bytes, 0, len);
        try {
            client_id = InetAddress.getByAddress(client_bytes);
        }
        catch (UnknownHostException e) {
            assert false;
        }
	seq = buffer.nextLong ();
        if (! put) {
            value_hash = new byte [20];
            buffer.nextBytes (value_hash, 0, 20);
        }
        if ((time_usec & 0x8000000000000000L) != 0) {
            secret_hash = new byte[20];
            buffer.nextBytes(secret_hash, 0, 20);
            time_usec &= 0x7fffffffffffffffL;
        }
    }

    public void serialize (OutputBuffer buffer) {
	super.serialize (buffer);
        if ((secret_hash == null)
            || java.util.Arrays.equals(secret_hash, ZERO_HASH))
            buffer.add (time_usec);
        else 
            buffer.add (0x8000000000000000L | time_usec);
	buffer.add (ttl_sec);
        buffer.add (guid);
        buffer.add (value.limit ());
        buffer.add (value.array (), value.arrayOffset (), value.limit ());
        buffer.add (put);
        byte [] client_bytes = client_id.getAddress ();
        buffer.add (client_bytes.length);
        buffer.add (client_bytes, 0, client_bytes.length);
        buffer.add (seq);
        if (! put) 
            buffer.add (value_hash, 0, 20);
        if ((secret_hash != null)
            && !java.util.Arrays.equals(secret_hash, ZERO_HASH))
            buffer.add(secret_hash, 0, 20);
    }

    public Object clone () throws CloneNotSupportedException {
        PutOrRemoveMsg result = (PutOrRemoveMsg) super.clone ();
        result.time_usec = time_usec;
        result.ttl_sec = ttl_sec;
        result.guid = guid;
        result.value = value;
        result.put = put;
        result.client_id = client_id;
        result.seq = seq;
        result.value_hash = value_hash;
        return result;
    }

    public String toString () {
	StringBuffer result = new StringBuffer (100);
	result.append ("(PutOrRemoveMsg super=");
	result.append (super.toString ());
	result.append (" time_usec="); result.append (time_usec);
	result.append (" ttl="); result.append(ttl_sec);
	result.append (" guid=");
	result.append (GuidTools.guid_to_string (guid));
	result.append (" value=<>");
        result.append (" put="); result.append (put);
        result.append (" client_id=");
        result.append (client_id.getHostAddress ());
	result.append (" seq="); result.append (seq);
	result.append (")");
	return result.toString ();
    }
}

