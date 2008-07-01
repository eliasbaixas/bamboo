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
 * @version $Id: IterGetResp.java,v 1.1 2005/08/19 05:45:46 srhea Exp $
 */
public class IterGetResp implements QuickSerializable, ByteCount {

    // Either get back this:
    public LinkedList<Pair<NeighborInfo,VirtualCoordinate>> neighbors;

    // Or all of these:
    public Set<NodeId> replicas;
    public Set<NodeId> synced;
    public LinkedList<Pair<Key,ByteBuffer>> values;
    public boolean allRead;

    public int maxvals;
    public NodeId client;

    public boolean recordByteCount() { return true; }
    public String byteCountKey() { 
        return client + " 0x" + Integer.toHexString(maxvals & 0xffff0000);
    }

    public IterGetResp(LinkedList<Pair<NeighborInfo,VirtualCoordinate>> n, 
                       Set<NodeId> r, Set<NodeId> s, 
                       LinkedList<Pair<Key,ByteBuffer>> v, boolean a,
                       int m, NodeId c) {
        assert (n == null && r != null && v != null && s != null) 
            || (n != null && !n.isEmpty() 
                && v == null && r == null && s == null);
        neighbors = n; replicas = r; synced = s; values = v; allRead = a;
        maxvals = m; client = c; 
    }

    public IterGetResp(InputBuffer buffer) throws QSException {
        int cnt = buffer.nextInt();
        if (cnt > 0) {
            neighbors = new LinkedList<Pair<NeighborInfo,VirtualCoordinate>>();
            while (cnt-- > 0) {
                NeighborInfo ni = new NeighborInfo(buffer);
                VirtualCoordinate vc = null; 
                if (buffer.nextBoolean())
                    vc = new VirtualCoordinate(buffer);
                neighbors.addLast(Pair.create(ni, vc));
            }
        }
        else {
            cnt *= -1;
            replicas = new LinkedHashSet<NodeId>();
            while (cnt-- > 0)
                replicas.add(new NodeId(buffer));
            cnt = buffer.nextInt();
            synced = new LinkedHashSet<NodeId>();
            while (cnt-- > 0)
                synced.add(new NodeId(buffer));
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
        }
        maxvals = buffer.nextInt();
        client = new NodeId(buffer);
    }

    public void serialize(OutputBuffer buffer) {
        if (neighbors != null) {
            buffer.add(neighbors.size());
            for (Pair<NeighborInfo,VirtualCoordinate> p : neighbors) {
                p.first.serialize(buffer);
                if (p.second == null)
                    buffer.add(false);
                else {
                    buffer.add(true);
                    p.second.serialize(buffer);
                }
            }
        }
        else {
            buffer.add(-1 * replicas.size());
            for (NodeId n : replicas)
                n.serialize(buffer);
            buffer.add(synced.size());
            for (NodeId n : synced)
                n.serialize(buffer);
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
        buffer.add(maxvals);
        client.serialize(buffer);
    }
}

