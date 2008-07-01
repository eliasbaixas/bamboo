/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht.fssim;
import java.util.LinkedList;
import java.util.Random;

public class RandomAlgorithm extends Algorithm {

    protected Random rand;

    public void process_pending_puts (LinkedList puts) {
        if (! puts.isEmpty ()) {
            int which = rand.nextInt (puts.size ());
            PutInfo accepted = null;
            while (! puts.isEmpty ()) {
                PutInfo pi = (PutInfo) puts.removeFirst ();
                if (which == 0)
                    accepted = pi;
                pi.cb.cb (which == 0, false);
                which--;
            }
            accept_put (accepted);
        }
    }

    public RandomAlgorithm (long p, long t, long seed) {
        super (p, t);
        rand = new Random (seed);
    }
}
 
