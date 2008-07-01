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

public class FairStorageAlgorithm extends Algorithm {

    protected Random rand;

    public void process_pending_puts (LinkedList puts) {

        if (puts.isEmpty ())
            return;

        // Find all the clients that are trying to do a put.  

        Iterator i = puts.iterator ();
        HashSet put_clients = new HashSet ();
        while (i.hasNext ()) {
            PutInfo pi = (PutInfo) i.next ();
            Integer client = new Integer (pi.client);
            put_clients.add (client);
	}

        PriorityQueue pq = new PriorityQueue (put_clients.size ());

        int t = 0;
	i = put_clients.iterator ();
        while (i.hasNext ()) {
            Integer client = (Integer) i.next ();
	    Long count = (Long) storage_by_client.get (client);
	    long c = (count == null) ? 0 : count.longValue ();
            // System.err.println ("client " + client + " storing " + c);
	    t += c;
	    pq.add (client, c);
        }

        // Find all the clients in the lowest non-empty fair sharing level.
        // In other words, decrement n from the number of clients until we
        // find some client(s) that are storing less than 1/nth of the total
        // storage in use.
        
        /*
        int n = pq.size ();
        HashSet below = new HashSet ();
        while (true) {
            double nth = ((double) t) / n;
            //System.err.println (sim.now_ms () + " " + t + " " + n + " " + nth
                    //+ " " + pq.getFirstPriority ());
            while ((! pq.isEmpty ()) && (pq.getFirstPriority () <= nth)) {
                Integer client = (Integer) pq.getFirst ();
                t -= pq.getFirstPriority ();
                n -= 1;
                assert t >= 0;
                assert n >= 0;
                pq.removeFirst ();
                if (put_clients.contains (client)) {
                    below.add (client);
                    // System.err.println ("client " + client + " below");
                }
            }
            if (! below.isEmpty ())
                break;
        }

        // Pick one of the clients in the lowest non-empty fair-sharing level
        // at random and accept one of its puts.

        int which = rand.nextInt (below.size ());
        Integer accepted_client = null;
        i = below.iterator ();
        while (which-- >= 0) 
            accepted_client = (Integer) i.next ();
            */
        Integer accepted_client = (Integer) pq.getFirst ();

        // System.err.println ("chose client " + accepted_client);
        PutInfo accepted = null;
        i = puts.iterator ();
        while (i.hasNext ()) {
            PutInfo pi = (PutInfo) i.next ();
            if ((accepted == null) && (pi.client==accepted_client.intValue())) {
                accepted = pi;
                pi.cb.cb (true, false);
            }
            else 
                pi.cb.cb (false, false);
        }
        accept_put (accepted);
    }

    public FairStorageAlgorithm (long p, long t, long seed) {
        super (p, t);
        rand = new Random (seed);
    }
}
 
