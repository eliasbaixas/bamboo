/*
 * Copyright (c) 2005 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht;
import bamboo.vivaldi.VirtualCoordinate;
import java.math.BigInteger;
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
 * @version $Id: IterGetReq.java,v 1.1 2005/08/19 05:45:46 srhea Exp $
 */
public class IterGetReq implements QuickSerializable, ByteCount {
    public BigInteger key;
    public int maxvals;
    public Key placemark;
    public long seq;
    public VirtualCoordinate vc;
    public NodeId client;

    public boolean recordByteCount() { return true; }
    public String byteCountKey() { 
        return client + " 0x" + Integer.toHexString(maxvals & 0xffff0000);
    }

    public IterGetReq(BigInteger k, int m, Key p, long s, 
                      VirtualCoordinate v, NodeId c) {
        key = k; maxvals = m; placemark = p; seq = s; vc = v; client = c;
        assert(placemark != null);
    }

    public IterGetReq(InputBuffer buffer) throws QSException {
        key = buffer.nextBigInteger();
        maxvals = buffer.nextInt();
        if (maxvals <= 0) 
            throw new QSException("maxvals <= 0");
        if (buffer.nextBoolean())
            placemark = new Key(buffer);
        else
            placemark = ZERO_KEY;
        seq = buffer.nextLong();
        if (buffer.nextBoolean())
            vc = new VirtualCoordinate(buffer);
        client = new NodeId(buffer);
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
        if (vc == null)
            buffer.add(false);
        else {
            buffer.add(true);
            vc.serialize(buffer);
        }
        client.serialize(buffer);
    }
}

