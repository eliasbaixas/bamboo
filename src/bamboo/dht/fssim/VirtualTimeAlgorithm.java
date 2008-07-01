/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht.fssim;
import bamboo.dht.IonTree;
import bamboo.lss.PriorityQueue;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

public class VirtualTimeAlgorithm extends Algorithm {

    protected static final boolean DEBUG = false;
    protected static final long MAX_TTL = 3600;
    protected static final int  MAX_SIZE = 1024;
    protected static final long MAX_PUT = MAX_TTL * MAX_SIZE;
    protected static final long RATE = 1024; // 1 KB/s
    protected static final long DISK_SIZE = RATE * MAX_TTL;
    protected static final long DISK_PAD = 1024;

    protected long virtual_time;
    protected HashMap latest_finish_times = new HashMap ();
    protected HashMap commitments_granted = new HashMap ();
    protected IonTree storage_tree = new IonTree (0, RATE, DISK_SIZE+DISK_PAD);
    protected long again_responses, cap_responses, accept_responses;

    protected void all_puts_expired (int client) {
        latest_finish_times.remove (new Integer (client));
    }

    protected long start_time (int client) {
        Long lft = (Long) latest_finish_times.get (new Integer (client));
        if (lft == null)
            // return 0;
            return virtual_time;
        else 
            // return lft.longValue (); 
            return Math.max (virtual_time, lft.longValue ());
    }

    public void print_usage () {
        super.print_usage ();
        if (DEBUG) System.err.print (sim.now_ms ());
        for (int i = 0; i < client_count; ++i) {
            Integer client = new Integer (i);
            Long count = (Long) commitments_granted.get (client);
            if (DEBUG) System.err.print (" " + 
                    (count == null ? 0 : count.longValue ()));
        }
        if (DEBUG) System.err.println ();
    }

    public void process_pending_puts (LinkedList puts) {
        storage_tree.shift_time (sim.now_ms () / 1000);
        assert storage_tree.accept_put (MAX_TTL, MAX_SIZE);
        if (! puts.isEmpty ()) {
            PriorityQueue pq = new PriorityQueue (puts.size ());
            HashMap second_puts = new HashMap ();
            if (DEBUG) System.err.println (
                    sim.now_ms () + ": system time is " + virtual_time);
            for (Iterator i = puts.iterator (); i.hasNext (); ) {
                PutInfo pi = (PutInfo) i.next ();
                assert pi.ttl_sec <= MAX_TTL;
                Integer client = new Integer (pi.client);
                LinkedList seconds = (LinkedList) second_puts.get (client);
                if (seconds == null) {
                    second_puts.put (client, new LinkedList ());
                    long ft = start_time (pi.client) + pi.size * pi.ttl_sec;
                    pq.add (pi, ft);
                    if (DEBUG) System.err.println (sim.now_ms () + 
                            ": put from client " + client + 
                            " has finish time " + ft);
                }
                else {
                    seconds.addLast (pi);
                }
            }

            while (! pq.isEmpty ()) {
                long ft = pq.getFirstPriority ();
                PutInfo pi = (PutInfo) pq.removeFirst ();
                Integer client = new Integer (pi.client);
                if (storage_tree.add_put (pi.ttl_sec, pi.size)) {
                    if (DEBUG) System.err.println (sim.now_ms () + 
                            ": accept put from client " + 
                            pi.client + " with finish time " + ft);
                    pi.cb.cb (true, false); 
                    ++accept_responses;
                    accept_put (pi);
                    latest_finish_times.put (client, new Long (ft));
                    Long cg = (Long) commitments_granted.get (client);
                    long ncg = ((cg == null) ? 0 : cg.longValue ()) + 
                        pi.size * pi.ttl_sec;
                    commitments_granted.put (client, new Long (ncg));
                    virtual_time = Math.max(ft - MAX_PUT - 1, virtual_time);
                }
                else {
                    if (DEBUG) System.err.println (sim.now_ms () + 
                            ": reject put from client " + 
                            pi.client + " with finish time " + ft);
                    Long lft = (Long) latest_finish_times.get (client);
                    boolean again = // false; /*
                        (lft == null) || (lft.longValue () < virtual_time); 
                    pi.cb.cb (false, again);
                    if (again) {
                        ++again_responses;
                    }
                    else {
                        ++cap_responses;
                    }
                }
                LinkedList seconds = (LinkedList) second_puts.get (client);
                if (! seconds.isEmpty ()) {
                    PutInfo npi = (PutInfo) seconds.removeFirst ();
                    long nft = start_time (npi.client) + npi.size * npi.ttl_sec;
                    pq.add (npi, nft);
                    if (DEBUG) System.err.println (sim.now_ms () + 
                            ": extra put from client " + client + 
                            " has finish time " + nft);
                }
            }
        }
    }

    public void simulation_finished () {
        System.err.println ("accept_responses=" + accept_responses);
        System.err.println ("again_responses=" + again_responses);
        System.err.println ("cap_responses=" + cap_responses);
    }

    public VirtualTimeAlgorithm (long p, long t) { super (p, t); }
}
 
