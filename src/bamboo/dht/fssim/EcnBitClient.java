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

public class EcnBitClient extends RandomUtil {
    protected long next_new_put;
    public EcnBitClient (final int client, final long period, 
                         final int size, final int ttl_sec, 
                         long start, final long stop_time, long seed) {
        final Random rand = new Random (seed);
        final Simulator sim = Simulator.instance; 
        final Algorithm alg = Algorithm.instance;
        sim.register_event (start, new Simulator.EventCb () {
                public void cb (Object not_used) {
                    alg.enqueue_put (client, size, ttl_sec, 
                        new Algorithm.PutResultCb () {
                            public void cb (boolean success, boolean again) {
                                if ((! success) && again) {
                                    if (sim.now_ms () < next_new_put) {
                                        alg.enqueue_put (client, size, ttl_sec, 
                                            this, null);
                                    }
                                }
                            }
                        }, null);
                    long next_wait = Math.round (Math.ceil (random_gaussian (
                                period, period * 0.1, rand)));
                    next_new_put = sim.now_ms () + next_wait;
                    if (sim.now_ms () + next_wait < stop_time) {
                        sim.register_event (next_wait, this, null);
                    }
                }}, null);
    }
}
