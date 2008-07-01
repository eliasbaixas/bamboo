/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht.fssim;
import java.util.LinkedList;
import java.util.HashMap;

public class Test7 {
    public static void main (String [] args) {
        long seed = 1;
        long stop_time = 5*60*60*1000;
        int ttl_sec = 3600;
        Simulator sim = new Simulator ();
        new VirtualTimeAlgorithm (1000, stop_time);
        int max_clients = 100;
        int client = 0;
        new FixedSubmitRateClient (client++, 500, 1024, ttl_sec, 1, 
                                   stop_time, seed++);
        new FixedSubmitRateClient (client++, 1000, 1024, ttl_sec, 1, 
                                   stop_time, seed++);
        for (int i = 0; i < 1; ++i) {
            new BurstyClient (client++, 1000, 1024, 600, 
                              600*1000, 3*3600*1000/2, 
                              1, stop_time, seed++);
        }
        sim.main_loop ();
    }
}
