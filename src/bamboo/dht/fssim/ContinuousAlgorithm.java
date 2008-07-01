/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht.fssim;
import bamboo.fst.Tree;
import bamboo.lss.PriorityQueue;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import static java.lang.Math.max;

public class ContinuousAlgorithm extends Algorithm {

    protected static final boolean DEBUG = false;
    protected static final boolean FF_TO_SYS_TIME = true;
    protected static final boolean INIT_STORAGE_TREE = false;

    public static class Params {
        protected long MAX_TTL = 3600;
        protected int  MAX_SIZE = 1024;
        protected long MAX_PUT = MAX_TTL * MAX_SIZE;
        protected long MAX_QUEUE = MAX_PUT; // * 10;
        protected long LOSE_IT_PAD = MAX_PUT + 1;
        protected long RATE = 1; // 1 byte per ms
        protected long TTL_MULT = 1000;
        protected long DISK_SIZE = RATE * MAX_TTL * TTL_MULT;
        protected long BURSTY_ADVANTAGE = MAX_PUT;
    }
    protected Params params;

    protected long virtual_time;
    protected Tree storage_tree;
    protected HashMap<Integer,Long> commitments_granted = 
        new HashMap<Integer,Long> ();

    /////////////////////////////////////////////////////////////////////////
    //
    // Keep track of the total size of all scheduled (but not yet accepted)
    // puts for each client.
    //
    /////////////////////////////////////////////////////////////////////////

    protected HashMap<Integer,Long> queue_size = new HashMap<Integer,Long> ();
    protected long queue_size (Integer client) {
        Long result = queue_size.get (client);
        return (result == null) ? 0 : result.longValue ();
    }
    protected void set_queue_size (Integer client, long value) {
        queue_size.put (client, new Long (value));
    }

    /////////////////////////////////////////////////////////////////////////
    //
    // Keep track of the latest finish time for each client.
    //
    /////////////////////////////////////////////////////////////////////////

    protected HashMap<Integer,Long> latest_finish_times = 
        new HashMap<Integer,Long> ();

    protected long latest_finish_time (Integer client) {
        Long lft = latest_finish_times.get (client);
        return (lft == null) ? 0 : lft.longValue ();
    }

    protected void set_latest_finish_time (Integer client, long value) {
        latest_finish_times.put (client, new Long (value));
    }

    /////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////

    public static class MyPutInfo extends PutInfo {
        public long start_time;
        public long commitment () { return size * ttl_sec; }
        public long finish_time () { return start_time + commitment (); }
        public MyPutInfo (int c, int sz, int t, 
                          PutResultCb r, Object u, long st) {
            super (c, sz, t, r, u);
            start_time = st;
        }
    }

    protected HashMap<Integer,LinkedList<MyPutInfo>> queue = 
        new HashMap<Integer,LinkedList<MyPutInfo>> ();
    protected MyPutInfo waiting_put;
    protected long waiting_ms;

    /////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////

    public void enqueue_put(int c, int size, int ttl_sec, PutResultCb cb, 
                            Object user_data) {

        long now_ms = sim.now_ms ();

        if (DEBUG) System.err.println (sim.now_ms () + 
                ": got new put from client " + c);

        // Increment the client count if necessary.
        client_count = max (client_count, c + 1);

        // See if this new put fits.
        Integer client = new Integer (c);
        long commitment = ttl_sec * size;
        long new_queue_size = queue_size (client) + commitment;

        if (new_queue_size <= params.MAX_QUEUE) {
            set_queue_size (client, new_queue_size);
            long ff_time = FF_TO_SYS_TIME ? 
                (virtual_time - params.BURSTY_ADVANTAGE) : 0;

            if (ff_time > latest_finish_time (client)) {
                if (DEBUG) System.err.println (sim.now_ms () + 
                        ": fast forwarding client " + client);
            } 
            long st = max (latest_finish_time (client), ff_time);

            if (DEBUG) System.err.println (sim.now_ms () + 
                    ": virtual time is " + virtual_time + 
                    " start time is " + st);

            set_latest_finish_time (client, st + commitment);
            MyPutInfo pi = new MyPutInfo (c, size, ttl_sec, cb, user_data, st);
            LinkedList<MyPutInfo> ll = queue.get (client);
            if (ll == null) {
                ll = new LinkedList<MyPutInfo> ();
                queue.put (client, ll);
            }
            ll.addLast (pi);

            if ((waiting_put == null) || (st < waiting_put.start_time)) {
                // Preempt the waiting put.
                waiting_put = null;
                process_queue ();
            }
        }
        else {
            // Reject the put.
            if (DEBUG) System.err.println (sim.now_ms () + 
                    ": rejecting put from client " + c);
            cb.cb (false, false);
        }
    }

    /////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////

    public void process_queue () {
        assert ! queue.isEmpty ();

        long now_ms = sim.now_ms ();
        storage_tree.shiftTime (now_ms);

        PriorityQueue pq = new PriorityQueue (queue.size ());
        for (Integer client : queue.keySet ()) {
            LinkedList<MyPutInfo> ll = queue.get (client);
            MyPutInfo pi = ll.getFirst ();
            pq.add (pi, pi.start_time);
        }
        while (! pq.isEmpty ()) {
            MyPutInfo pi = (MyPutInfo) pq.removeFirst ();
            long ttl_ms = pi.ttl_sec * params.TTL_MULT;
            if (storage_tree.addPut (ttl_ms, pi.size)) {
                accept_put (pi);
                Integer client = new Integer (pi.client);
                LinkedList<MyPutInfo> ll = queue.get (client);
                ll.removeFirst ();
                if (ll.isEmpty ())
                    queue.remove (client);
                else {
                    pi = ll.getFirst ();
                    pq.add (pi, pi.start_time);
                }
            }
            else {
                waiting_ms = storage_tree.nextAccept (ttl_ms, pi.size);
                sim.register_event (waiting_ms - now_ms, waiting_cb, null);
                waiting_put = pi;
                if (DEBUG) System.err.println (sim.now_ms () + 
                        ": waiting until " + waiting_ms + 
                        " to accept put from client " + waiting_put.client);
                break;
            }
        }
        assert queue.isEmpty () || (waiting_put != null);
    }

    protected Simulator.EventCb waiting_cb = new Simulator.EventCb () {
        public void cb (Object not_used) { 
            long now_ms = sim.now_ms ();
            if ((waiting_put != null) && (now_ms >= waiting_ms)) {
                if (DEBUG) System.err.println (sim.now_ms () + 
                        ": accepting waiting put from client " +
                        waiting_put.client);
                storage_tree.shiftTime (now_ms);
                long ttl_ms = waiting_put.ttl_sec * params.TTL_MULT;
                boolean r = storage_tree.addPut (ttl_ms, waiting_put.size);
                assert r;
                accept_put (waiting_put);
                Integer client = new Integer (waiting_put.client);
                LinkedList<MyPutInfo> ll = queue.get (client);
                ll.removeFirst ();
                if (ll.isEmpty ())
                    queue.remove (client);
                waiting_put = null;
                if (! queue.isEmpty ())
                    process_queue (); 
            }
        }
    };

    public void print_usage () {
        super.print_usage ();
        /*
        System.out.print (sim.now_ms ());
        for (int i = 0; i < client_count; ++i) {
            Integer client = new Integer (i);
            Long count = (Long) commitments_granted.get (client);
            System.out.print (" " + 
                    (count == null ? 0 : count.longValue ()));
        }
        System.out.println ();
        */
        // System.err.println ("tree height = " + storage_tree.height ());
    }

    protected void accept_put (final PutInfo pi) {
        // Make sure only the version that takes a MyPutInfo is called.
        assert false; 
    }
 
    protected void accept_put (MyPutInfo pi) {

        Integer client = new Integer (pi.client);

        set_queue_size (client, queue_size (client) - pi.commitment ());

        virtual_time = max (virtual_time, pi.start_time);

        // Increment the client's commitments granted.
        Long cg = commitments_granted.get (client);
        long ncg = ((cg == null) ? 0 : cg.longValue ()) + 
            pi.size * pi.ttl_sec;
        commitments_granted.put (client, new Long (ncg));

        // Tell the client it was accepted.
        pi.cb.cb (true, false); 

        super.accept_put (pi);
    }

    protected void init (long st) {
        instance = this;
        sim = Simulator.instance;
        storage_tree = new Tree (0, params.RATE, 
                params.DISK_SIZE + params.MAX_SIZE - 1);
        if (INIT_STORAGE_TREE) {
            for (long i = 1; i < params.MAX_TTL; ++i) {
                boolean r = storage_tree.addPut (
                        i * params.TTL_MULT, params.MAX_SIZE);
                assert r : i;
            }
        }
        stop_time = st; 
        sim.register_event (st, 
                new Simulator.EventCb () {
                    public void cb (Object not_used) {
                        simulation_finished ();
                    }
                }, null);
        sim.register_event (10000, 
                new Simulator.EventCb () {
                    public void cb (Object not_used) {
                        if (print_usages && (sim.now_ms () < stop_time)) {
                            print_usage ();
                            sim.register_event (10000, this, null);
                        }
                    }
                }, null);
    }

    public ContinuousAlgorithm (long st) { 
        params = new Params ();
        init (st);
    }

    public ContinuousAlgorithm (long st, Params par) { 
        params = par;
        init (st);
    }

    // Don't use this in the continuous algorithm.

    public void process_pending_puts (LinkedList puts) {
        throw new NoSuchMethodError ();
    }
}
 
