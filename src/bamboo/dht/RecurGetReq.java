/*
 * Copyright (c) 2005 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht;
import bamboo.vivaldi.VirtualCoordinate;
import java.math.BigInteger;
import java.util.LinkedHashSet;
import java.util.Set;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;
import static bamboo.db.StorageManager.Key;
import static bamboo.db.StorageManager.ZERO_KEY;
import static bamboo.lss.UdpCC.ByteCount;

/**
 * Get request type.
 *
 * @author Sean C. Rhea
 * @version $Id: RecurGetReq.java,v 1.1 2005/08/19 05:45:46 srhea Exp $
 */
public class RecurGetReq implements QuickSerializable, ByteCount {
    public BigInteger key;
    public int maxvals;
    public Key placemark;
    public long seq;
    public NodeId client;
    public NodeId return_addr;

    public boolean recordByteCount() { return true; }
    public String byteCountKey() { 
        return client + " 0x" + Integer.toHexString(maxvals & 0xffff0000);
    }

    public RecurGetReq(BigInteger k, int m, Key p, long s, 
                       NodeId c, NodeId r) {
        key = k; maxvals = m; placemark = p; seq = s; client = c; return_addr = r;
        assert(placemark != null);
    }

    public RecurGetReq(InputBuffer buffer) throws QSException {
        key = buffer.nextBigInteger();
        maxvals = buffer.nextInt();
        if (maxvals <= 0) 
            throw new QSException("maxvals <= 0");
        if (buffer.nextBoolean())
            placemark = new Key(buffer);
        else
            placemark = ZERO_KEY;
        seq = buffer.nextLong();
        client = new NodeId(buffer);
        return_addr = new NodeId(buffer);
    }

    public void serialize(OutputBuffer buffer) {
        buffer.add(key);
        buffer.add(maxvals);
        if (placemark == ZERO_KEY)
            buffer.add(false);
        else {
            buffer.add(true);
            placemark.serialize(buffer);
        }
        buffer.add(seq);
        client.serialize(buffer);
        return_addr.serialize(buffer);
    }
}

