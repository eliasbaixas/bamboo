/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht.fssim;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.Random;
import bamboo.util.RandomUtil;

public class BurstyClient extends RandomUtil {

    public int client;
    public long next_mode_switch;
    public boolean on;
    public Random rand;
    public Simulator sim;
    public Algorithm alg;
    public long period;
    public int size;
    public int ttl_sec;
    public long on_time;
    public long off_time;
    public long stop_time;

    public Simulator.EventCb on_cb = new Simulator.EventCb () {
        public void cb (Object not_used) {
            if (sim.now_ms () == next_mode_switch) {
                // System.err.println (sim.now_ms () + ": turning on");
                next_mode_switch = sim.now_ms () + on_time; /* Math.round (
                        Math.ceil (random_gaussian (on_time, 
                                on_time * 0.1, rand))); */
            }

            alg.enqueue_put (client, size, ttl_sec, 
                    new Algorithm.PutResultCb () {
                    public void cb (boolean success, boolean again) {}
                    }, null);

            long next_wait = Math.round (Math.ceil (random_gaussian (
                            period, period * 0.1, rand)));

            if (sim.now_ms () + next_wait < stop_time) {
                if (sim.now_ms () + next_wait < next_mode_switch) {
                    sim.register_event (next_wait, this, null);
                }
                else {
                    // System.err.println (sim.now_ms () + ": turning off");
                    next_mode_switch = sim.now_ms () + off_time; /* Math.round (
                            Math.ceil (random_gaussian (off_time, 
                                    off_time * 0.1, rand))); */
                    sim.register_event (next_mode_switch - sim.now_ms (), 
                                        this, null);
                }
            }
        }
    };

    public BurstyClient (int client, long period, int size, int ttl_sec, 
                         long on_time, long off_time, 
                         long start, long stop_time, long seed) {

        this.period = period;
        this.client = client;
        this.size = size;
        this.ttl_sec = ttl_sec;
        this.on_time = on_time;
        this.off_time = off_time;
        this.stop_time = stop_time;

        sim = Simulator.instance; 
        alg = Algorithm.instance;
        rand = new Random (seed);
        next_mode_switch = start + off_time; /* Math.round (
                Math.ceil (random_gaussian (off_time, off_time * 0.1, rand)));*/

        sim.register_event (next_mode_switch - sim.now_ms (), on_cb, null);
    }
}
