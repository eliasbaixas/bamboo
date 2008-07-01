/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht.fssim;
import java.util.LinkedList;
import java.util.HashMap;

public class Test2 {
    /*public static long [] start_times = { 
	0, 0, 2*3600*1000, 4*3600*1000, 4*3600*1000 
    };
    public static long [] stop_times  = { 
	10*3600*1000, 6*3600*1000, 8*3600*1000, 8*3600*1000, 10*3600*1000 
    };*/
    public static long [] start_times = { 
	0, 2*3600*1000, 4*3600*1000, 6*3600*1000
    };
    public static long [] stop_times  = { 
	16*3600*1000, 16*3600*1000, 16*3600*1000, 16*3600*1000
    };
    public static void main (String [] args) {
        long seed = 1;
        long stop_time = 16*3600*1000;
        int ttl_sec = 3*3600;
        Simulator sim = new Simulator ();
        if (args [0].equals ("control"))
            new RandomAlgorithm (1000, stop_time, seed++);
        else if (args [0].equals ("rate"))
            new FairRateAlgorithm (1000, stop_time, seed++, false);
        else if (args [0].equals ("storage"))
            new FairStorageAlgorithm (1000, stop_time, seed++);
        else if (args [0].equals ("commitment"))
            new FairCommitmentAlgorithm (1000, stop_time, seed++);
        else if (args [0].equals ("virtual"))
            new VirtualTimeAlgorithm (1000, stop_time);
        else if (args [0].equals ("noqueue"))
            new NoQueuingAlgorithm (1000, stop_time);
        else {
            System.err.println ("mode " + args[0] + " not supported");
            System.exit (1);
        }
	for (int i = 0; i < start_times.length; ++i) {
	    new EcnBitClient (i, 1000, 1024, ttl_sec, start_times [i],
		                      stop_times [i], seed++);
	}
        sim.main_loop ();
    }
}
