/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht.fssim;
import bamboo.lss.PriorityQueue;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Random;

public class Simulator {

    public static Simulator instance;

    protected PriorityQueue pq = new PriorityQueue (100);
    protected long now_ms;

    public static double random_exponential (double mean, Random rand) {
        double u = rand.nextDouble ();
        return (0 - (mean * Math.log (1.0 - u)));
    }

    public interface EventCb {
        void cb (Object user_data);
    }

    public void register_event (long delay_ms, EventCb cb, Object user_data) {
        pq.add (new Object [] {cb, user_data}, now_ms + delay_ms);
    }

    public long now_ms () {
        return now_ms;
    }

    public void main_loop () {
        while (! pq.isEmpty ()) {
            now_ms = pq.getFirstPriority ();
            Object [] pair = (Object []) pq.removeFirst ();
            EventCb cb = (EventCb) pair [0];
            cb.cb (pair [1]);
        }
    }

    public Simulator () {
        assert instance == null;
        instance = this;
    }
}
