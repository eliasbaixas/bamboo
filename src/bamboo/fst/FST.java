/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.fst;
import bamboo.lss.ASyncCore;
import bamboo.lss.ASyncCoreImpl;
import bamboo.lss.PriorityQueue;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.LineNumberReader;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Random;
import static java.lang.Math.max;
import static bamboo.util.Curry.*;

public class FST {

    protected static class PutInfo {
        public int size, ttl;
        public long startTime;
        public Client client;
        public Runnable cb;
        public PutInfo(int s, int t, long st, Client c, Runnable r) { 
            size = s; ttl = t; startTime = st; client = c; cb = r; 
        }
    }

    protected ASyncCore acore;
    protected Tree tree;

    protected long maxQueueSize;
    protected long virtualTime;
    protected long burstyAdvantage;

    protected long nextAcceptTime;
    protected PutInfo nextAcceptPutInfo;

    protected LinkedHashMap<Client,LinkedList<PutInfo>> queues = 
        new LinkedHashMap<Client,LinkedList<PutInfo>>();
    protected LinkedHashMap<Client,Long> queueSizes = 
        new LinkedHashMap<Client,Long>();
    protected LinkedHashMap<Client,Long> latestFinishTimes =
        new LinkedHashMap<Client,Long>();


    public FST(ASyncCore acore, int rateBytesPerSecond, long capacityBytes, 
               int maxPutBytes, int maxQueueSize, long burstyAdvantage) {
        this.acore = acore;
        this.maxQueueSize = maxQueueSize;
        this.burstyAdvantage = burstyAdvantage;
        int rateBytesPerMilli = rateBytesPerSecond / 1000; // round down
        tree = new Tree(acore.nowMillis(), rateBytesPerMilli,
                        capacityBytes + maxPutBytes);
    }

    /**
     * Calls cb when it's safe to accept the given put; returns false if the
     * client already has a full queue.
     */
    public boolean addPut(int sizeBytes, int ttlMillis, Client client, 
                          Runnable cb) {
        long commitment = (long) sizeBytes * ttlMillis;
        Long currentSize = queueSizes.get(client);
        if ((currentSize != null) 
            && (currentSize.longValue() + commitment > maxQueueSize)) {
            return false;
        }
        else {
            long newSize = commitment;
            LinkedList<PutInfo> queue = null;
            if (currentSize == null) {
                queue = new LinkedList<PutInfo>();
                queues.put(client, queue);
            }
            else {
                queue = queues.get(client);
                newSize += currentSize.longValue();
            }
            long startTime = max(latestFinishTime(client), 
                                 virtualTime - burstyAdvantage);
            latestFinishTimes.put(client, new Long(startTime + commitment));
            queue.addLast(
                    new PutInfo(sizeBytes, ttlMillis, startTime, client, cb));
            queueSizes.put(client, new Long(newSize));
            if ((nextAcceptPutInfo == null)
                || (startTime < nextAcceptPutInfo.startTime)) {
                nextAcceptPutInfo = null;
                processQueues();
            }
            return true;
        }
    }

    public void processQueues() {
        assert ! queues.isEmpty ();

        long now_ms = acore.nowMillis();
        tree.shiftTime(now_ms);

        PriorityQueue pq = new PriorityQueue(queues.size());
        for (Client client : queues.keySet()) {
            PutInfo pi = queues.get(client).getFirst();
            pq.add(pi, pi.startTime);
        }
        while (! pq.isEmpty()) {
            PutInfo pi = (PutInfo) pq.removeFirst();
            if (tree.addPut(pi.ttl, pi.size)) {
                LinkedList<PutInfo> ll = acceptPut(pi);
                if (! ll.isEmpty()) {
                    pi = ll.getFirst();
                    pq.add(pi, pi.startTime);
                }
            }
            else {
                nextAcceptTime = tree.nextAccept(pi.ttl, pi.size);
                nextAcceptPutInfo = pi;
                acore.registerTimer(nextAcceptTime - now_ms, waitDone);
                break;
            }
        }
        assert queues.isEmpty() || (nextAcceptPutInfo != null);
    }

    protected Runnable waitDone = new Runnable() {
        public void run() { 
            long now_ms = acore.nowMillis();
            if ((nextAcceptPutInfo != null) && (now_ms >= nextAcceptTime)) {
                tree.shiftTime(now_ms);
                boolean r = tree.addPut(nextAcceptPutInfo.ttl, 
                                        nextAcceptPutInfo.size);
                assert r;
                acceptPut(nextAcceptPutInfo);
                nextAcceptPutInfo = null;
                if (! queues.isEmpty())
                    processQueues(); 
            }
        }
    };

    protected LinkedList<PutInfo> acceptPut(PutInfo pi) {
        LinkedList<PutInfo> ll = queues.get(pi.client);
        ll.removeFirst();
        if (ll.isEmpty()) {
            queues.remove(pi.client);
            queueSizes.remove(pi.client);
        }
        else {
            queueSizes.put(pi.client, 
                    queueSizes.get(pi.client).longValue() - pi.size * pi.ttl);
        }
        virtualTime = max(virtualTime, pi.startTime);
        pi.cb.run();
        return ll;
    }

    protected long latestFinishTime(Client client) {
        Long lft = latestFinishTimes.get(client);
        return (lft == null) ? 0 : lft.longValue ();
    }

    public static class TestClient {
        public ASyncCore acore;
        public FST fst;
        public Client id;
        public Random rand;
        public int period_ms, size;
        public int ttl_sec, ttl_ms;
        public long bytesStored, commitmentsGranted;
        public long totalWait;
        public int accepts, rejects;
        public TestClient(ASyncCore a, FST f, Client i, int p, int s, int t,
                          int seed) {
            acore = a; fst = f; id = i; period_ms = p; size = s; 
            ttl_sec = t; ttl_ms = t * 1000; rand = new Random(seed);
        }
        public void scheduleNextPut() {
            long wait = 3*period_ms/4 + rand.nextInt(period_ms/2);
            acore.registerTimer(wait, doPut);
        }
        public Runnable doPut = new Runnable() {
            public void run() {
                boolean result = fst.addPut(size, ttl_ms, id, 
                        curry(putDone, new Long(acore.nowMillis())));
                /*
                System.err.println(acore.nowMillis() + " Client " + id 
                                   + " put size=" + size 
                                   + " ttl=" + ttl_sec 
                                   + (result ? " accepted" : " rejected"));
                */
                if (! result)
                    ++rejects;
                scheduleNextPut();
            }
        };
        public Thunk1<Long> putDone = new Thunk1<Long>() {
            public void run(Long startTime) {
                bytesStored += size;
                commitmentsGranted += size*ttl_sec;
                totalWait += acore.nowMillis() - startTime.longValue();
                ++accepts;
                acore.registerTimer(ttl_ms, putExpired);
            }
        };
        public Runnable putExpired = new Runnable() {
            public void run() { bytesStored -= size; }
        };
    }

    public static void main(String args[]) throws Exception {

        LineNumberReader reader = new LineNumberReader(new
                InputStreamReader(new FileInputStream(args[0])));

        String line = null;
        while(true) {
            line = reader.readLine();
            if (! line.startsWith("#"))
                break;
        }
        String [] fields = line.trim().split("  *");

        int maxTTL = Integer.parseInt(fields[0]);
        int maxPut = Integer.parseInt(fields[1]);
        int rateBps = Integer.parseInt(fields[2]);
        int seed = 1;
        final ASyncCore acore = new ASyncCoreImpl();

        FST fst = new FST(acore, rateBps, maxTTL*maxPut /* capacity */, 
                          maxPut, maxTTL*maxPut /* max queue size */, 
                          maxTTL*maxPut /* bursty advantage */);

        LinkedList<Double> params = new LinkedList<Double>();
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("#"))
                continue;
            fields = line.trim().split("  *");
            for (int i = 0; i < 3; ++i)
                params.addLast(new Double(Double.parseDouble(fields[i])));
        }

        int fairPeriod = rateBps / maxPut * 1000 * (params.size() / 3);

        final LinkedList<TestClient> clients = new LinkedList<TestClient>();
        for (Iterator<Double> i = params.iterator(); i.hasNext();) {
            int period = (int) (i.next().doubleValue() * fairPeriod);
            int size = (int) i.next().doubleValue();
            int ttl = (int) i.next().doubleValue();
            assert size <= maxPut : "size " + size + " > maxPut";
            assert ttl <= maxTTL : "ttl " + ttl + " > maxTTL";
            clients.addLast(new TestClient(acore, fst, 
                        new Client(new byte[]{(byte) seed}), 
                        period, size, ttl, seed++));
        }
        for (TestClient c : clients)
            c.scheduleNextPut();

        final long startTime = acore.nowMillis();
        acore.registerTimer(10*1000, new Runnable() {
                public void run() {
                    System.out.print(acore.nowMillis() - startTime);
                    for (TestClient c : clients) {
                        System.out.print(" " + c.commitmentsGranted + " " 
                                         + c.bytesStored + " " +
                                         + c.totalWait + " " + c.accepts + " "
                                         + c.rejects);
                        c.totalWait = 0; c.accepts = 0; c.rejects = 0;
                    }
                    System.out.println();
                    acore.registerTimer(10*1000, this);
                }
        });

        acore.async_main();
    }
}

