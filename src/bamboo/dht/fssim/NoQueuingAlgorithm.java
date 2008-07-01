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

public class NoQueuingAlgorithm extends Algorithm {

    protected static final boolean DEBUG = false;
    protected static final long MAX_TTL = 3600;
    protected static final int  MAX_SIZE = 1024;
    protected static final long MAX_PUT = MAX_TTL * MAX_SIZE;
    protected static final long RATE = 1024; // 1 KB/s
    protected static final long DISK_SIZE = RATE * MAX_TTL;

    protected long virtual_time;
    protected HashMap latest_finish_times = new HashMap ();
    protected IonTree storage_tree = new IonTree (0, RATE, DISK_SIZE);
    protected LinkedList accept_virtual_times = new LinkedList ();

    public void enqueue_put(int client, int size, int ttl_sec, 
                            final PutResultCb cb, Object user_data) {

        if (client + 1 > client_count)
            client_count = client + 1;

        // Step window forward to now, and find the lowest finishing time
        // during the last slot.

        long lowest_ft = Long.MAX_VALUE;
        Iterator i = accept_virtual_times.iterator ();
        while (i.hasNext ()) {
            Object [] pair = (Object []) i.next ();
            long then = ((Long) pair [0]).longValue ();
            long ft = ((Long) pair [1]).longValue ();
            if (then + period * 2 < sim.now_ms ())
                i.remove ();
            else 
                lowest_ft = Math.min (lowest_ft, ft);
        }

        long lft = -1;
        {
            Long l = (Long) latest_finish_times.get (new Integer (client));
            if (l != null) lft = l.longValue ();
        }
        long ft = start_time (client) + size * ttl_sec;
        if (DEBUG) System.err.print (sim.now_ms () + ": vt=" + virtual_time +
                " client=" + client + " last_ft=" + lft +
                " lowest_ft=" + lowest_ft + ", st=" + start_time (client) + 
                " ft=" + ft);

        // If the finishing time of this put is greater than the lowest
        // finishing time from the last slot, reject it immediately.

        if (ft > lowest_ft) {
            if (DEBUG) System.err.println (" CAPACITY");
            cb.cb (false, false); // CAPACITY
            return;
        }

        // Otherwise, accept it if the storage tree is okay with it.

        // System.err.println ("Before shift_time:\n" + storage_tree);
        storage_tree.shift_time (sim.now_ms () / 1000);
        // System.err.println ("After shift_time:\n" + storage_tree);
        if (storage_tree.add_put (ttl_sec, size)) {
            // System.err.println ("After accept:\n" + storage_tree);
            if (DEBUG) System.err.println (" SUCCESS");
            accept_put (new PutInfo (client, size, ttl_sec, cb, user_data));
            latest_finish_times.put (new Integer (client), new Long (ft));
            virtual_time = Math.max (ft - MAX_PUT - 1, virtual_time);
            Object [] pair = {new Long (sim.now_ms ()), new Long (ft)};
            accept_virtual_times.addLast (pair);
            cb.cb (true, false); // SUCCESS
            return;
        }
        // System.err.println ("After reject:\n" + storage_tree);

        // If they were below their fair share, but the storage tree didn't
        // have room, ask them to retry soon.

        if (DEBUG) System.err.println (" AGAIN");
        sim.register_event (1000, new Simulator.EventCb () {
                public void cb (Object not_used) {
                    cb.cb (false, true); // AGAIN
                }
            }, null);
    }

    protected void all_puts_expired (int client) {
        latest_finish_times.remove (new Integer (client));
    }

    protected long start_time (int client) {
        Long lft = (Long) latest_finish_times.get (new Integer (client));
        if (lft == null)
            return virtual_time;
        else 
            return Math.max (virtual_time, lft.longValue ());
    }

    public void process_pending_puts (LinkedList puts) {
        assert puts.isEmpty ();
    }

    public NoQueuingAlgorithm (long p, long t) { super (p, t); }
}
 
