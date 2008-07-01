/*
 * Copyright (c) 2005 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht;
import bamboo.router.NeighborInfo;
import bamboo.util.Pair;
import bamboo.vivaldi.VirtualCoordinate;
import java.nio.ByteBuffer;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Set;
import ostore.network.NetworkMessage;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;
import static bamboo.db.StorageManager.Key;
import static bamboo.dht.Dht.GetValue;
import static bamboo.lss.UdpCC.ByteCount;

/**
 * Either returns a list of neighbors to try instead, or a list of values and
 * a new placemark.
 *
 * @author Sean C. Rhea
 * @version $Id: RecurGetResp.java,v 1.1 2005/08/19 05:45:46 srhea Exp $
 */
public class RecurGetResp extends NetworkMessage implements ByteCount {

    public long seq;
    public Set<NodeId> replicas;
    public Set<NodeId> synced;
    public LinkedList<Pair<Key,ByteBuffer>> values;
    public boolean allRead;
    public NodeId thisReplica;

    public int maxvals;
    public NodeId client;

    public boolean recordByteCount() { return true; }
    public String byteCountKey() { 
        return client + " 0x" + Integer.toHexString(maxvals & 0xffff0000);
    }

    public RecurGetResp(NodeId dest, long q, Set<NodeId> r, Set<NodeId> s,
            LinkedList<Pair<Key,ByteBuffer>> v, boolean a,
            NodeId t, int m, NodeId c) {
        super(dest, false);
        seq = q; replicas = r; synced = s; values = v;
        allRead = a; thisReplica = t; maxvals = m; client = c; 
    }

    public RecurGetResp(InputBuffer buffer) throws QSException {
        super(buffer);
        seq = buffer.nextLong();
        int cnt = buffer.nextInt();
        replicas = new LinkedHashSet<NodeId>();
        synced = new LinkedHashSet<NodeId>();
        while (cnt-- > 0) {
            NodeId n = new NodeId(buffer);
            replicas.add(n);
            if (buffer.nextBoolean())
                synced.add(n);
        }
        cnt = buffer.nextInt();
        values = new LinkedList<Pair<Key,ByteBuffer>>();
        while (cnt-- > 0) {
            Key k = new Key(buffer);
            int len = buffer.nextInt();
            byte b[] = new byte[len];
            buffer.nextBytes(b, 0, len);
            values.addLast(Pair.create(k, ByteBuffer.wrap(b)));
        }
        allRead = buffer.nextBoolean();
        thisReplica = new NodeId(buffer);
        maxvals = buffer.nextInt();
        client = new NodeId(buffer);
    }

    public void serialize(OutputBuffer buffer) {
        super.serialize(buffer);
        buffer.add(seq);
        buffer.add(replicas.size());
        for (NodeId n : replicas) {
            n.serialize(buffer);
            buffer.add(synced.contains(n));
        }
        buffer.add(values.size());
        for (Pair<Key,ByteBuffer> p : values) {
            p.first.serialize(buffer);
            buffer.add(p.second.limit() - p.second.position());
            buffer.add(p.second.array(), 
                    p.second.arrayOffset() + p.second.position(),
                    p.second.limit() - p.second.position());
        }
        buffer.add(allRead);
        thisReplica.serialize(buffer);
        buffer.add(maxvals);
        client.serialize(buffer);
    }
}

