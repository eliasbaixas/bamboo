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

public class FairRateAlgorithm extends Algorithm {

    protected Random rand;
    protected int window_size;
    protected boolean requested;
    protected boolean use_last_accept = false;

    public static class ClientInfo {
        int total_accepted;
        int total_requested;
        int [] accepted;    
        int [] requested;    
        long last_accept;
        ClientInfo (int window_size) {
            accepted = new int [window_size];
            requested = new int [window_size];
        }
    }

    public HashMap clients = new HashMap ();

    public void process_pending_puts (LinkedList puts) {
        PriorityQueue pq = new PriorityQueue (clients.size ());

        // Find the rate that each client has been storing at and put it into
        // a priority queue.  The rate is calculated as the number of accepted
        // or requested puts over the last window of time (thirty seconds by
        // default).

        int widx = (int) ((sim.now_ms () / period) % window_size);
        Iterator i = clients.keySet ().iterator ();
        int t = 0;
        while (i.hasNext ()) {
            Integer client = (Integer) i.next ();
            ClientInfo cinfo = (ClientInfo) clients.get (client);
            cinfo.total_requested -= cinfo.requested [widx];
            cinfo.requested [widx] = 0;
            cinfo.total_accepted -= cinfo.accepted [widx];
            cinfo.accepted [widx] = 0;
            if (requested) {
                t += cinfo.total_requested;
                pq.add (client, cinfo.total_requested);
            }
            else {
                t += cinfo.total_accepted;
                pq.add (client, cinfo.total_accepted);
            }
        }
        if (puts.isEmpty ())
            return;

        // Find all the clients that are trying to do a put.  Also, add an
        // entry to the priority queue for every "new" client.

        i = puts.iterator ();
        HashSet new_clients = new HashSet ();
        HashSet put_clients = new HashSet ();
        while (i.hasNext ()) {
            PutInfo pi = (PutInfo) i.next ();
            Integer client = new Integer (pi.client);
            ClientInfo cinfo = (ClientInfo) clients.get (client);
            put_clients.add (client);
            if ((! clients.containsKey (client)) 
                && (! new_clients.contains (client))) {
                pq.add (client, 0);
                new_clients.add (client);
            }
        }

        // Find all the clients in the lowest non-empty fair sharing level.
        // In other words, decrement n from the number of clients until we
        // find some client(s) that are storing less than 1/nth of the total
        // storage in use.
        
        int n = pq.size ();
        HashSet below = new HashSet ();
        while (true) {
            double nth = ((double) t) / n;
            /*System.err.println (sim.now_ms () + " " + t + " " + n + " " + nth
                    + " " + pq.getFirstPriority ());*/
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

        // Pick one of the clients in the lowest non-empty fair-sharing level
        // at random and accept one of its puts.

        Integer accepted_client = null;
        if (use_last_accept) {
            PriorityQueue la = new PriorityQueue (below.size ());
            i = below.iterator ();
            while (i.hasNext ()) {
                Integer client = (Integer) i.next ();
                ClientInfo cinfo = (ClientInfo) clients.get (client);
                if (cinfo == null)
                    la.add (client, 0);
                else
                    la.add (client, cinfo.last_accept);
            }
            accepted_client = (Integer) la.getFirst ();
        }
        else {
            int which = rand.nextInt (below.size ());
            i = below.iterator ();
            while (which-- >= 0) 
                accepted_client = (Integer) i.next ();
        }

        PutInfo accepted = null;
        i = puts.iterator ();
        // System.err.print (sim.now_ms () + " " + accepted_client);
        while (i.hasNext ()) {
            PutInfo pi = (PutInfo) i.next ();
            if ((accepted == null) && (pi.client==accepted_client.intValue())) {
                accepted = pi;
                pi.cb.cb (true, false);
            }
            else 
                pi.cb.cb (false, false);
            // System.err.print (" " + pi.client);
        }
        // System.err.println ("");
        accept_put (accepted);

        ClientInfo cinfo = (ClientInfo) clients.get (accepted_client);
        if (cinfo == null) {
            cinfo = new ClientInfo (window_size);
            clients.put (accepted_client, cinfo);
        }
        cinfo.last_accept = sim.now_ms ();
        cinfo.accepted [widx] = accepted.size;
        cinfo.total_accepted += accepted.size;
        i = puts.iterator ();
        while (i.hasNext ()) {
            PutInfo pi = (PutInfo) i.next ();
            Integer client = new Integer (pi.client);
            cinfo = (ClientInfo) clients.get (client);
            if (cinfo == null) {
                cinfo = new ClientInfo (window_size);
                clients.put (client, cinfo);
            }
            cinfo.requested [widx] += 1;
            cinfo.total_requested += 1;
        }
     }

    public FairRateAlgorithm (long p, long t, long seed, boolean r) {
        super (p, t);
        rand = new Random (seed);
        window_size = 30*1000 / ((int) period);
        requested = r;
    }
}
 
