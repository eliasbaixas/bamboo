/*
 * Copyright (c) 2005 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht;
import bamboo.util.Pair;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;
import static bamboo.db.StorageManager.Key;

/**
 * Get request type.
 *
 * @author Sean C. Rhea
 * @version $Id: RpcGetResp.java,v 1.2 2005/06/03 05:14:42 srhea Exp $
 */
public class RpcGetResp implements QuickSerializable {
    public LinkedList<Pair<Key,ByteBuffer>> values;
    public boolean allRead;
    public RpcGetResp(LinkedList<Pair<Key,ByteBuffer>> v, boolean a) { 
        values = v; allRead = a;
    }
    public RpcGetResp(InputBuffer buffer) throws QSException {
        int count = buffer.nextInt();
        values = new LinkedList<Pair<Key,ByteBuffer>>();
        while (count-- > 0) {
            Key k = new Key(buffer);
            byte[] vb = new byte[buffer.nextInt()];
            buffer.nextBytes(vb, 0, vb.length);
            values.addLast(Pair.create(k, ByteBuffer.wrap(vb)));
        }
        allRead = buffer.nextBoolean();
    }
    public void serialize(OutputBuffer buffer) {
        buffer.add(values.size());
        for (Pair<Key,ByteBuffer> p : values) {
            p.first.serialize(buffer);
            buffer.add(p.second.limit() - p.second.position());
            buffer.add(p.second.array(), 
                    p.second.arrayOffset() + p.second.position(),
                    p.second.limit() - p.second.position());
        }
        buffer.add(allRead);
    }
}


