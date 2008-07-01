/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.util;
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
 * An set with an attached LRU ordering.
 * 
 * @author Sean C. Rhea
 * @version $Id: LruMap.java,v 1.2 2004/02/10 21:26:26 srhea Exp $
 */
public class LruMap {

    protected static class Node {
        public Node pred, succ;
        public Object key, value;
        public Node (Object k, Object v) { key = k; value = v; }
    }

    protected int capacity;
    protected Map map;
    protected Node first, last;

    public LruMap (int cap, Map backing_map) {
        if (! backing_map.isEmpty ())
            throw new IllegalArgumentException ("backing_map not empty");
        capacity = cap;
        map = backing_map;
    }

    public int size () { return map.size (); }

    public boolean isEmpty () { return map.isEmpty (); }

    public Map backing_map () { return map; }

    protected static class MyIterator implements Iterator {
        private Node current;
        public MyIterator (Node n) {
            current = n;
        }
        public Object next () {
            if (current == null) 
                throw new NoSuchElementException ();
            Object result = current.value;
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
     * Returns an iterator which walks through the set of items in the set,
     * starting with the most recently added.
     */
    public Iterator iterator () {
        return new MyIterator (first);
    }

    public void put (Object key, Object value) {
        if (capacity == 0) 
            return;

        Node n = null;
        if (map.containsKey (key)) {
            n = (Node) map.get (key);
            // If we already have this node, take it out of the current
            // position in the list...
            rem_n (n);
            // Update the value...
            n.value = value;
        }
        else {
            // otherwise, we have to add this node, so possibly...
            if (map.size () >= capacity) {
                // ... make space for it ...
                if (map.remove (last.key) == null)
                    BUG ("in ll but not in ts");
                last = last.pred;
                last.succ = null;
            }
            // ... add it to the map ...
            n = new Node (key, value);
            map.put (key, n);
        }
        // ... then add it to the first of the list.
        add_n (n);
    }

    public Object get (Object key) {
        Node n = null;
        if (map.containsKey (key)) {
            n = (Node) map.get (key);
            // If we already have this node, take it out of the current
            // position in the list then add it to the first of the list.
            rem_n (n);
            add_n (n);
            return n.value;
        }
        else {
            return null;
        }
    }

    public Object lastKey () {
        if (last == null) 
            throw new NoSuchElementException ();
        return last.key;
    }

    public Object remove (Object key) {
        Node n = (Node) map.remove (key);
        if (n == null) 
            return null;
        else {
            rem_n (n);
            return n.value;
        }
    }

    protected void rem_n (Node n) {
        if (first == n) first = n.succ;
        if (last == n) last = n.pred;

        if (n.pred != null) n.pred.succ = n.succ;
        if (n.succ != null) n.succ.pred = n.pred;
    }

    protected void add_n (Node n) {
        n.succ = first;
        n.pred = null;
        if (first != null) first.pred = n;
        first = n;
        if (last == null)
            last = first;
    }

    protected void BUG (String msg) {
        System.err.println (msg);
        Thread.dumpStack ();
        System.exit (1);
    }
}
