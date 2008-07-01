/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht.fssim;
import java.util.LinkedList;
import java.util.HashMap;

public class Test1 {
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
        new EcnBitClient (0,   100, 1024, ttl_sec, 1, stop_time, seed++);
        new EcnBitClient (1,  1000, 1024, ttl_sec, 1, stop_time, seed++);
        new EcnBitClient (2, 10000, 1024, ttl_sec, 1, stop_time, seed++);
        new EcnBitClient (3, 10000, 1024, ttl_sec, 1, stop_time, seed++);
        new EcnBitClient (4, 20000, 1024, ttl_sec, 1, stop_time, seed++);
        new EcnBitClient (5, 20000, 1024, ttl_sec, 1, stop_time, seed++);
        new EcnBitClient (6, 40000, 1024, ttl_sec, 1, stop_time, seed++);
        new EcnBitClient (7, 40000, 1024, ttl_sec, 1, stop_time, seed++);
        new EcnBitClient (8, 80000, 1024, ttl_sec, 1, stop_time, seed++);
        new EcnBitClient (9, 80000, 1024, ttl_sec, 1, stop_time, seed++);
        sim.main_loop ();
    }
}
