/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht;
import bamboo.util.USecTimer;
import java.util.Random;

/**
 * Tree to keep track of whether storage is less than capacity.
 *
 * @author Sean C. Rhea
 * @version $Id: StorageTree.java,v 1.4 2004/07/16 18:38:47 srhea Exp $
 */
public class StorageTree {

    public long rate;
    public long range;
    public int branching;
    public Node root;

    public class Node {
        public boolean valid;
        public long low;
        public long high;
        public long min_free;
        public long offset;
        public Node [] children;

        public void create_children () {
            for (int index = 0; index < branching; ++index) {
                Node child = children [index] = new Node ();
                long spacing = (high - low) / branching;
                child.low = low + spacing * index;
                child.high = child.low + spacing;
                if (child.low + 1 == child.high) {
                    child.min_free = 1;
                    child.valid = true;
                }
                else {
                    child.children = new Node [branching];
                    child.create_children ();
                }
            }
            compute_min_free ();
        }

        public void compute_min_free () {
            if (low + 1 == high)
                return;
            boolean was_valid = valid;
            valid = true;
            int invalid_index = 0;
            for (int index = 0; index < branching; ++index) {
                Node child = children [index];
                if (((index < branching - 1) 
                     && (child.low > children [index+1].low)) 
                    || (! child.valid)) {
                    valid = false;
                    invalid_index = index;
                }
            }
            if (valid) {
                low = children [0].low;
                high = children [branching - 1].high;
                long to_assign = (high - low) / branching * rate;
                min_free = Long.MAX_VALUE;
                for (int index = 0; index < branching; ++index) {
                    Node child = children [index];
                    long this_child_min = child.min_free - child.offset 
                        + to_assign * (branching - index - 1);
                    min_free = Math.min (this_child_min, min_free);
                }
            }
            else if (was_valid) {
                for (int index = invalid_index+1; index < branching; ++index) {
                    Node child = children [index];
                    child.offset += offset;
                }
                offset = 0;
            }
        }

        public void shift_time (long new_low) {
            if (low + 1 == high) {
                min_free = 1;
                offset = 0;
                valid = true;
                low += range;
                high = low + 1;
            }
            else {
                long dest = new_low - 1;
                for (int index = 0; index < branching; ++index) {
                    Node child = children [index];
                    child.offset += offset;
                    if ((child.high >= dest) && (dest >= child.low))
                        child.shift_time (new_low);
                }
                offset = 0;
                compute_min_free ();
            }
        }

        public boolean accept_put (long now, long exp) {
            // System.out.println ("accept(" + now + "," + exp + "), valid=" + 
            //         valid + " low=" + low + " high=" + high);
            if (valid && (low >= now) && (high <= exp)) {
                return offset + 1 <= min_free + (range - high + now) * rate;
            }
            else if (valid && (low >= exp)) {
                return true;
            }
            for (int index = 0; index < branching; ++index) {
                Node child = children [index];
                if (! child.accept_put (now, exp))
                    return false;
            }
            return true;
        }

        public void add_put (long now, long exp) {
            if (valid && (low >= now) && (high <= exp)) {
                offset += 1;
            }
            else if (valid && (low >= exp)) {
                // do nothing
            }
            else {
                for (int index = 0; index < branching; ++index) {
                    Node child = children [index];
                    child.add_put (now, exp);
                }
            }
            compute_min_free ();
        }

        public String toString (String prefix) {
            String result = prefix + "Node low=" + low + " high=" + high + 
                " min_free=" + min_free + " offset=" + offset + " valid=" +
                valid;
            if (children == null) 
                result += " has no children.";
            else {
                result += " has children:";
                for (int index = 0; index < branching; ++index) {
                    Node child = children [index];
                    result += "\n" + child.toString (prefix + "  ");
                }
            }
            return result;
        }

        public String toString () { return toString (""); }
    }

    public void shift_time (long new_low) {
        root.shift_time (new_low);
    }

    public StorageTree (long low, long high, long rate) {
        this.rate = rate;
        range = high - low;
        branching = 2;
        root = new Node ();
        root.low = low;
        root.high = high;
        root.children = new Node [branching];
        root.create_children ();
    }

    public static void main (String [] args) {
        Random rand = new Random (1);
        for (int round = 1; round <= 2; ++round) {
            System.out.println ("round " + round);
            long start, delta;
            int size = Integer.parseInt (args [0]);
            StorageTree tree = new StorageTree (0, size, 1);
            int put_ttls_len = 0;
            for (int i = 1; i <= size; i *= 2)
                put_ttls_len++;
            // System.out.println ("len=" + put_ttls_len);
            int [] put_ttls = new int [put_ttls_len];
            for (int i = 1, j = 0; i <= size; i *= 2) {
                // System.out.println ("ttl[" + j + "]=" + i);
                put_ttls [j++] = i;
            }
            for (int step = 0; step < size; ++step) {
                if ((round == 1) && (step >= 500))
                    break;
                System.gc ();
                for (int i = put_ttls_len - 1; i >= 0; --i) {
                    int ttl = rand.nextInt (put_ttls [i]) + 1;
                    start = USecTimer.currentTimeMicros ();
                    // System.out.println (tree.root);
                    boolean accept = tree.root.accept_put (step, step + ttl);
                    delta = USecTimer.currentTimeMicros () - start;
                    if (round == 2)
                        System.out.println ("accept " + delta);
                    if (accept) {
                        start = USecTimer.currentTimeMicros ();
                        tree.root.add_put (step, step + ttl);
                        delta = USecTimer.currentTimeMicros () - start;
                        if (round == 2)
                            System.out.println ("add " + delta);
                    }
                }
                start = USecTimer.currentTimeMicros ();
                tree.root.shift_time (step + 1);
                delta = USecTimer.currentTimeMicros () - start;
                if (round == 2)
                    System.out.println ("shift " + delta);
            }
        }
    }
}

