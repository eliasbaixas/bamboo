/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht.fssim;
import java.util.LinkedList;
import java.util.HashMap;

public abstract class Algorithm {

    public static Algorithm instance;

    protected LinkedList<PutInfo> pending_puts = new LinkedList<PutInfo> ();
    protected Simulator sim;
    protected long period;
    protected long stop_time;
    public boolean print_usages = true;
    public HashMap<Integer,Long> storage_by_client = 
        new HashMap<Integer,Long> ();
    protected long total_puts;
    protected int client_count;

    protected static class PutInfo {
        int client, size, ttl_sec;
        PutResultCb cb;
        Object user_data;
        PutInfo (int c, int s, int t, PutResultCb r, Object u) {
            client = c; size = s; ttl_sec = t; cb = r; user_data = u;
        }
    }

    public interface PutResultCb {
        void cb (boolean success, boolean again);
    }

    public void enqueue_put(int client, int size, int ttl_sec, PutResultCb cb, 
                            Object user_data) {
        if (client + 1 > client_count)
            client_count = client + 1;
        pending_puts.addLast(new PutInfo(client, size, ttl_sec, cb, user_data));
    }

    protected void remove_put (PutInfo pi) {
        Integer client = new Integer (pi.client);
        Long count = (Long) storage_by_client.get (client);
        assert count != null;
        long new_count = count.longValue () - pi.size;
        storage_by_client.put (client, new Long (new_count));
        --total_puts;
        if (new_count == 0)
            all_puts_expired (pi.client);
        // System.err.println (sim.now_ms () + ": client " + client + 
        //        " storage decreased by " + pi.size + " bytes");
    }

    protected void all_puts_expired (int client) {
    }

    protected void accept_put (final PutInfo pi) {
        pi.cb = null; pi.user_data = null;
        Integer client = new Integer (pi.client);
        Long count = (Long) storage_by_client.get (client);
        if (count == null)
            count = new Long (pi.size);
        else
            count = new Long (count.longValue () + pi.size);
        storage_by_client.put (client, count);
        ++total_puts;
        sim.register_event (pi.ttl_sec*1000, new Simulator.EventCb () {
                public void cb (Object not_used) {
                    remove_put (pi);
                }}, null);
    }

    public void print_usage () {
        System.out.print (sim.now_ms ());
        for (int i = 0; i < client_count; ++i) {
            Integer client = new Integer (i);
            Long count = (Long) storage_by_client.get (client);
            System.out.print (" " + (count == null ? 0 : count.longValue ()));
        }
        System.out.println ();
    }

    public abstract void process_pending_puts (LinkedList puts);

    public void simulation_finished () {}

    public Algorithm () {}

    public Algorithm (long p, long t) {
        instance = this;
        period = p; stop_time = t; 
        sim = Simulator.instance;
        sim.register_event (period, new Simulator.EventCb () {
                public void cb (Object not_used) {
                    LinkedList pp = pending_puts;
                    pending_puts = new LinkedList ();
                    process_pending_puts (pp);
                    if (sim.now_ms () < stop_time) 
                        sim.register_event (period, this, null);
                    else
                        simulation_finished ();
                    if (print_usages && (sim.now_ms () % 10000 == 0))
                        print_usage ();
                }}, null);
    }
}
