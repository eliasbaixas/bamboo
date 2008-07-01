/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht.fssim;
import java.util.LinkedList;
import java.util.HashMap;

public class Test6 {
    public static void main (String [] args) {
        long seed = 1;
        long stop_time = 5*60*60*1000;
        int ttl_sec = 10;
        Simulator sim = new Simulator ();
        new VirtualTimeAlgorithm (1000, stop_time);
        int max_clients = 100;
        int client = 0;
        new FixedSubmitRateClient (client++, 100, 1024, 
                                   ttl_sec, 1, stop_time, seed++);
        for (int i = 0; i < max_clients; ++i) {
            new FixedAcceptRateClient (client++, 2*max_clients*1000, 
                    1024, ttl_sec, 1, stop_time, seed++, 1000);
        }
        sim.main_loop ();
    }
}
