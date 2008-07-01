/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.util;
import java.util.NoSuchElementException;
import java.util.Vector;

/**
 * A non-blocking priority queue. 
 *
 * @author     Westley Weimer
 * @version    $Id: PriorityQueue.java,v 1.1 2005/02/16 05:57:13 srhea Exp $
 */
public class PriorityQueue<Value, Priority extends Comparable<Priority>> {

    protected Vector<Value> heap;     // the vector in which we embed the heap
    protected Vector<Priority> prio;  // priorities
    protected int heap_size;          // current heap size

    /**
     * Creates a new priority queue.
     */
    public PriorityQueue () {
	heap = new Vector<Value> ();
	prio = new Vector<Priority> ();
	heap_size = 0;
    }

    /**
     * Creates a new priority queue with the given initial capacity.
     */
    public PriorityQueue (int initial_capacity) {
	int capacity = initial_capacity + 1; // compensate for skipping index 0.
	heap = new Vector<Value> (capacity);
	prio = new Vector<Priority> (capacity);
	heap_size = 0;
    }

    public final int size () {
	return heap_size;
    }

    public final boolean isEmpty () {
	return heap_size == 0;
    }

    /**
     * Adds an object with the given priority.
     */
    public final boolean add (Value o, Priority p) {
        // Make sure we have a minimum element in index 0.
        if (prio.isEmpty ()) {
           prio.add (p);
        }
        else {
            Priority min = prio.elementAt(0);
            if (min == null || p.compareTo(min) < 0) 
                prio.setElementAt(p, 0);
        }
	int i = ++heap_size;
        heap.setSize (i+1);
        prio.setSize (i+1);
	while (i > 1 && (prio.elementAt(i/2).compareTo (p) > 0)) {
	    heap.setElementAt (heap.elementAt(i/2), i);
	    prio.setElementAt (prio.elementAt(i/2), i);
	    i = i/2;
	}
        heap.setElementAt (o, i);
        prio.setElementAt (p, i);
	return true;
    }

    public final Priority getFirstPriority () {
	if (heap_size < 1)
            throw new NoSuchElementException ();
	return prio.elementAt (1);
    }

    public final Value getFirst () {
	if (heap_size < 1)
	    throw new NoSuchElementException ();
	return heap.elementAt (1);
    }

    public final Value removeFirst () {
	if (heap_size < 1) 
	    throw new NoSuchElementException ();

	Value result = heap.elementAt (1);
	heap.setElementAt(heap.elementAt(heap_size), 1);
	heap.setElementAt(null, heap_size);  // allow for gc
	prio.setElementAt(prio.elementAt(heap_size), 1);
	prio.setElementAt(null, heap_size);  // allow for gc
	--heap_size;

        // heapify

	int l, r, smallest, parent = 1;
	while (true) {
	    l = parent * 2;
	    r = parent * 2 + 1;

	    // Is the left child's priority smaller than it's parent's?
	    if (l <= heap_size 
                && prio.elementAt(l).compareTo(prio.elementAt(parent)) < 0) 
		smallest = l;
	    else
		smallest = parent;

	    // Is the right child's priority smaller than both?
	    if (r <= heap_size 
                && prio.elementAt(r).compareTo(prio.elementAt(smallest)) < 0)
		smallest = r;

	    // If neither is smaller, we're done.
	    if (smallest == parent)
                break;

	    // Otherwise, swap the parent and the child of smaller priority.
	    Value temp_o = heap.elementAt(parent);
	    Priority temp_p = prio.elementAt(parent);
	    heap.setElementAt(heap.elementAt(smallest), parent);
	    prio.setElementAt(prio.elementAt(smallest), parent);
	    heap.setElementAt(temp_o, smallest);
	    prio.setElementAt(temp_p, smallest);

	    // Recurse.
	    parent = smallest;
	}

        if (heap_size == 0)
            prio.setElementAt(null, 0);  // allow for gc 

	return result;
    }

    public String toString () {
	String result = "(PriorityQueue";
	for (int i = 1; i <= heap_size; ++i) 
	    result += "\n  (" + prio.elementAt(i) + ", " 
                + heap.elementAt(i) + ")";
	return result + ")";
    }
}

