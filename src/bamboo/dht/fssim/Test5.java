/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht.fssim;
import java.util.LinkedList;
import java.util.HashMap;

public class Test5 {
    public static void main (String [] args) {
        long seed = 1;
        long stop_time = 5*60*60*1000;
        int ttl_sec = 60*60;
        Simulator sim = new Simulator ();
        if (args [0].equals ("control"))
            new RandomAlgorithm (1000, stop_time, seed++);
        else if (args [0].equals ("rate"))
            new FairRateAlgorithm (1000, stop_time, seed++, false);
        else if (args [0].equals ("storage"))
            new FairStorageAlgorithm (1000, stop_time, seed++);
        else if (args [0].equals ("virtual"))
            new VirtualTimeAlgorithm (1000, stop_time);
        else if (args [0].equals ("noqueue"))
            new NoQueuingAlgorithm (1000, stop_time);
        else {
            System.err.println ("only control, rate, and storage supported");
            System.exit (1);
        }
        int j = 0;
        for (int i = 2; i <= 32; i*=2) {
            new EcnBitClient (j++, 2000, 1024, i*60, 1, stop_time, seed++);
        }
        new EcnBitClient (j++, 2000, 1024, 3600, 1, stop_time, seed++);
        sim.main_loop ();
    }
}
