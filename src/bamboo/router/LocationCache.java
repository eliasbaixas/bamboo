/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.router;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import ostore.util.Carp;
import ostore.util.NodeId;
import bamboo.api.BambooNeighborInfo;
import bamboo.util.GuidTools;

/**
 * A Chord-like location cache; for use in performance evaluations.
 * 
 * @author Sean C. Rhea
 * @version $Id: LocationCache.java,v 1.9 2003/10/05 23:26:52 srhea Exp $
 */
public class LocationCache {

    protected BigInteger MODULUS;
    
    protected static class Node {
        public Node pred, succ;
        public NeighborInfo ni;
        public Node (NeighborInfo n) { ni = n; }
    }

    protected int capacity;
    protected SortedMap cache = new TreeMap ();
    protected Node first, last;

    public LocationCache (int cap, BigInteger mod) {
	MODULUS = mod;
        capacity = cap;
    }

    public int size () {
        return cache.size ();
    }

    protected static class MyIterator implements Iterator {
        private Node current;
        public MyIterator (Node n) {
            current = n;
        }
        public Object next () {
            if (current == null) 
                throw new NoSuchElementException ();
            NeighborInfo result = current.ni;
            current = current.succ;
            return result;
        }
        public boolean hasNext () {
            return current != null;
        }
        public void remove () {
            throw new UnsupportedOperationException ();
        }
    }

    /**
     * Returns an iterator which walks through the set of nodes in the cache,
     * starting with the most recently added.
     */
    public Iterator iterator () {
        return new MyIterator (first);
    }

    public void add_node (NeighborInfo ni) {
        if (capacity == 0) 
            return;

        Node n = null;
        if (cache.containsKey (ni.guid)) {
            n = (Node) cache.get (ni.guid);
            // If we already have this node, take it out of the current
            // position in the list...
            rem_n (n);
        }
        else {
            // otherwise, we have to add this node, so possibly...
            if (cache.size () >= capacity) {
                // ... make space for it ...
                if (cache.remove (last.ni.guid) == null)
                    BUG ("in ll but not in ts");
                last = last.pred;
                last.succ = null;
            }
            // ... add it to the map ...
            n = new Node (ni);
            cache.put (ni.guid, n);
        }
        // ... then add it to the first of the list.
        n.succ = first;
        n.pred = null;
        if (first != null) first.pred = n;
        first = n;
        if (last == null)
            last = first;
    }

    public boolean remove_node (NeighborInfo ni) {
        Node n = (Node) cache.remove (ni.guid);
        if (n == null) 
            return false;
        else {
            rem_n (n);
            return true;
        }
    }

    public String toString () {
        if (cache.isEmpty ()) 
            return "(LocationCache <empty>)";

        StringBuffer result = new StringBuffer (cache.size () * 100);
        Iterator j = cache.values ().iterator ();
        result.append ("(Location Cache\n  Sorted by guid:\n");
        while (j.hasNext ()) {
            Node n = (Node) j.next ();
            result.append ("    ");
            result.append (n.ni);
            result.append ("\n");
        }
        result.append ("  Sorted by use order (first is most recent):\n");
        Node n = first;
        while (n != null) {
            result.append ("    ");
            result.append (n.ni);
            result.append ("\n");
            n = n.succ;
        }
        result.append (")\n");
        return result.toString ();
    }

    public NeighborInfo closest_node (BigInteger guid) {
        if (cache.isEmpty ()) 
            return null;

        if (cache.containsKey (guid))
            return ((Node) cache.get (guid)).ni;

        SortedMap hm = cache.headMap (guid);
        SortedMap tm = null;
        if (hm.isEmpty () || (tm = cache.tailMap (guid)).isEmpty ()) {

            // This guid is smaller (or larger) than any in the map.  Thus the
            // closest one in the map is either the smallest (or largest)
            // or--in case of wrap-around--the largest (or smallest).
                
            BigInteger largest_key = (BigInteger) cache.lastKey ();
            BigInteger smallest_key = (BigInteger) cache.firstKey ();
            BigInteger largest_dist = calc_dist (guid, largest_key);
            BigInteger smallest_dist = calc_dist (guid, smallest_key);
            if (largest_dist.compareTo (smallest_dist) < 0)
                return ((Node) cache.get (largest_key)).ni;
            else 
                return ((Node) cache.get (smallest_key)).ni;
        }
        else {

            // tm contains all nodes strictly greater than guid, hm contains
            // all nodes strictly less than guid.  Thus guid is between
            // tm.firstKey () and hm.lastKey ().

            BigInteger largest_key = (BigInteger) tm.firstKey ();
            BigInteger smallest_key = (BigInteger) hm.lastKey ();
            BigInteger largest_dist = calc_dist (guid, largest_key);
            BigInteger smallest_dist = calc_dist (guid, smallest_key);
            if (largest_dist.compareTo (smallest_dist) < 0)
                return ((Node) tm.get (largest_key)).ni;
            else 
                return ((Node) hm.get (smallest_key)).ni;
        }
    }

    protected BigInteger calc_dist (BigInteger a, BigInteger b) {
        return GuidTools.calc_dist (a, b, MODULUS);
    }

    protected void rem_n (Node n) {
        if (first == n) first = n.succ;
        if (last == n) last = n.pred;

        if (n.pred != null) n.pred.succ = n.succ;
        if (n.succ != null) n.succ.pred = n.pred;
    }

    protected void BUG (String msg) {
        System.err.println (msg);
        Thread.dumpStack ();
        System.exit (1);
    }
}

