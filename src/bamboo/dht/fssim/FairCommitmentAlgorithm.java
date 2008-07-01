/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht.fssim;
import bamboo.lss.PriorityQueue;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

public class FairCommitmentAlgorithm extends Algorithm {

    protected Random rand;
    protected HashMap commitments = new HashMap ();

    public void process_pending_puts (LinkedList puts) {

        if (! puts.isEmpty ()) {

            // Find all the clients that are trying to do a put.  

            Iterator i = puts.iterator ();
            HashSet put_clients = new HashSet ();
            while (i.hasNext ()) {
                PutInfo pi = (PutInfo) i.next ();
                Integer client = new Integer (pi.client);
                put_clients.add (client);
            }

            // Sort them by commitment.

            PriorityQueue pq = new PriorityQueue (put_clients.size ());

            long t = 0;
            i = put_clients.iterator ();
            while (i.hasNext ()) {
                Integer client = (Integer) i.next ();
                Long count = (Long) commitments.get (client);
                long c = (count == null) ? 0 : count.longValue ();
                /*System.err.println ("client " + client + " commit is " +
                 * c);*/
                t += c;
                pq.add (client, c);
            }

            // Find all the clients in the lowest non-empty fair sharing
            // level.  In other words, decrement n from the number of clients
            // until we find some client(s) that are storing less than 1/nth
            // of the total storage in use.

            int n = pq.size ();
            HashSet below = new HashSet ();
            while (true) {
                double nth = ((double) t) / n;
                /*System.err.println (sim.now_ms () + " " + t + " " + n + " " + 
                  nth + " " + pq.getFirstPriority ());*/
                while ((! pq.isEmpty ()) && (pq.getFirstPriority () <= nth)) {
                    Integer client = (Integer) pq.getFirst ();
                    t -= pq.getFirstPriority ();
                    n -= 1;
                    assert t >= 0;
                    assert n >= 0;
                    pq.removeFirst ();
                    if (put_clients.contains (client))
                        below.add (client);
                }
                if (! below.isEmpty ())
                    break;
            }

            // Pick one of the clients in the lowest non-empty fair-sharing
            // level at random and accept one of its puts.

            int which = rand.nextInt (below.size ());
            Integer accepted_client = null;
            i = below.iterator ();
            while (which-- >= 0) 
                accepted_client = (Integer) i.next ();

            PutInfo accepted = null;
            i = puts.iterator ();
            while (i.hasNext ()) {
                PutInfo pi = (PutInfo) i.next ();
                if ((accepted == null) && 
                    (pi.client==accepted_client.intValue())) {
                    accepted = pi;
                    pi.cb.cb (true, false);
                }
                else 
                    pi.cb.cb (false, false);
            }
            accept_put (accepted);

            // Increase the accepted client's commitment.
            Long commitment = (Long) commitments.get (accepted_client);
            long curc = (commitment == null) ? 0 : commitment.longValue ();
            long newc = curc + (long) accepted.ttl_sec * 1000 * accepted.size;
            assert newc >= 0;
            /*System.err.println ("client " + accepted_client + 
                    " commit=" + curc + " ttl=" + accepted.ttl_sec + 
                    " size=" + accepted.size + " new commit=" + newc);*/
            commitments.put (accepted_client, new Long (newc));
        }

        // Decrease all clients' commitments.

        Iterator i = commitments.keySet ().iterator ();
        while (i.hasNext ()) {
            Integer client = (Integer) i.next ();
            Long commitment = (Long) commitments.get (client);
            Long storage = (Long) storage_by_client.get (client);
            /*System.err.println ("client " + client + " commit=" +
                    commitment.longValue () + " store=" + storage.longValue
                    ());*/
            long newc = commitment.longValue () - storage.longValue () * period;
            if (newc < 0) newc = 0;
            assert newc >= 0;
            commitments.put (client, new Long (newc));
            /*System.err.println (sim.now_ms () + " reducing client " + 
              client + " commit to " + newc); */
        }
    }

    public FairCommitmentAlgorithm (long p, long t, long seed) {
        super (p, t);
        rand = new Random (seed);
    }
}
 
