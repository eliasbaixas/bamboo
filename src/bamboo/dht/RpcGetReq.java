/*
 * Copyright (c) 2005 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht;
import java.math.BigInteger;
import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;
import static bamboo.db.StorageManager.Key;
import static bamboo.db.StorageManager.ZERO_KEY;

/**
 * Get request type.
 *
 * @author Sean C. Rhea
 * @version $Id: RpcGetReq.java,v 1.1 2005/05/18 23:01:25 srhea Exp $
 */
public class RpcGetReq implements QuickSerializable {
    public BigInteger key;
    public int maxvals;
    public Key placemark;
    public RpcGetReq(BigInteger k, int m, Key p) {
        key = k; maxvals = m; placemark = p;
        assert(placemark != null);
    }
    public RpcGetReq(InputBuffer buffer) throws QSException {
        key = buffer.nextBigInteger();
        maxvals = buffer.nextInt();
        if (maxvals <= 0) 
            throw new QSException("maxvals <= 0");
        if (buffer.nextBoolean())
            placemark = new Key(buffer);
        else
            placemark = ZERO_KEY;
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
    }
}

