/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht.fssim;
import java.util.LinkedList;
import java.util.HashMap;

public class Test4 {
    public static void main (String [] args) {
        long seed = 1;
        long st = 5*60*60*1000;
        int ttl_sec = 60*60;
        Simulator sim = new Simulator ();
        if (args [0].equals ("control"))
            new RandomAlgorithm (1000, st, seed++);
        else if (args [0].equals ("rate"))
            new FairRateAlgorithm (1000, st, seed++, false);
        else if (args [0].equals ("storage"))
            new FairStorageAlgorithm (1000, st, seed++);
        else if (args [0].equals ("virtual"))
            new VirtualTimeAlgorithm (1000, st);
        else if (args [0].equals ("noqueue"))
            new NoQueuingAlgorithm (1000, st);
        else {
            System.err.println ("only control, rate, and storage supported");
            System.exit (1);
        }
        int client = 0;
        /*
        for (int i = 0; i < 3; ++i) {
        new EcnBitClient(client++,   100,    1024,    3600,    1, st, seed++);
        new EcnBitClient(client++,  1000,    1024,    3600,    1, st, seed++);
        }
        for (int i = 0; i < 100; ++i) {
        new EcnBitClient(client++,  1000,    1,    3600,    1, st, seed++);
        }
        */
        new EcnBitClient(client++,   50,    1024,    3600,    1, st, seed++);
        new EcnBitClient(client++,  500,    1024,    3600,    1, st, seed++);
        new EcnBitClient(client++, 1000,    1024,    3600,    1, st, seed++);
        new EcnBitClient(client++, 1000/4,  1024,    3600/4,  1, st, seed++);
        new EcnBitClient(client++, 1000/12, 1024,    3600/12, 1, st, seed++);
        new EcnBitClient(client++, 1000/4,  1024/4,  3600,    1, st, seed++);
        new EcnBitClient(client++, 1000/16, 1024/16, 3600,    1, st, seed++);
        sim.main_loop ();
    }
}
