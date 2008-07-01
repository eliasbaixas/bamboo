/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.lss;
import java.util.NoSuchElementException;

/**
 * A non-blocking priority queue. 
 *
 * @author     Westley Weimer
 * @version    $Id: PriorityQueue.java,v 1.7 2004/05/14 10:03:42 harlanyu Exp $
 */
public class PriorityQueue {

    protected Object [] heap;	// the vector in which we embed the heap
    protected long [] prio;	// priorities
    protected int capacity;	// maximum size
    protected int heap_size;	// current heap size

    /**
     * Creates a new priority queue with the give initial capacity.
     */
    public PriorityQueue (int initial_capacity) {
	capacity = initial_capacity + 1; // compensate for skipping index 0.
	heap = new Object[capacity];
	prio = new long[capacity];
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
    public final boolean add (Object o, long p) {
	if (heap_size >= capacity - 1) {
	    int new_capacity = capacity << 1;
	    Object [] new_heap = new Object [new_capacity];
	    long [] new_prio   = new long [new_capacity];
	    System.arraycopy (heap, 0, new_heap, 0, capacity);
	    System.arraycopy (prio, 0, new_prio, 0, capacity);
	    heap = new_heap;
	    prio = new_prio;
	    capacity = new_capacity;
	}

	int i = ++heap_size;
	while (i > 1 && prio [i/2] > p) {
	    heap [i] = heap [i/2];
	    prio [i] = prio [i/2];
	    i = i/2;
	}
	heap [i] = o;
	prio [i] = p;
	return true;
    }

    public final long getFirstPriority () {
	if (heap_size < 1)
	    return 0;
	return prio [1];
    }

    public final Object getFirst () {
	if (heap_size < 1)
	    throw new NoSuchElementException ();
	return heap [1];
    }

    public final long getIndexPriority(int i) {
	if (heap_size < i)
	    return 0;
	return prio [i];
    }

    public final Object getIndex(int i) {
	if (heap_size < i)
	    throw new NoSuchElementException ();
	return heap [i];
    }

    public final boolean contains(Object o) {
	for (int i = 1; i <= size(); i++) {
	    if (getIndex(i).equals(o)) {
		return true;
	    }
	}
	return false;
    }

    public final Object removeFirst () {
	if (heap_size < 1) 
	    throw new NoSuchElementException ();
	Object result = heap [1];
	heap [1] = heap [heap_size];
	heap [heap_size] = null;  // allow for gc
	prio [1] = prio [heap_size];
	--heap_size;

        // heapify

	int l, r, smallest, parent = 1;
	while (true) {
	    l = parent * 2;
	    r = parent * 2 + 1;

	    // Is the left child's priority smaller than it's parent's?
	    if (l <= heap_size && prio [l] < prio [parent]) 
		smallest = l;
	    else
		smallest = parent;

	    // Is the right child's priority smaller than both?
	    if (r <= heap_size && prio [r] < prio [smallest])
		smallest = r;

	    // If neither is smaller, we're done.
	    if (smallest == parent)
                break;

	    // Otherwise, swap the parent and the child of smaller priority.
	    Object temp_o = heap [parent];
	    long temp_p = prio [parent];;
	    heap [parent] = heap [smallest];
	    prio [parent] = prio [smallest];
	    heap [smallest] = temp_o;
	    prio [smallest] = temp_p;

	    // Recurse.
	    parent = smallest;
	}

	return result;
    }

    public String toString () {
	String result = "(PriorityQueue";
	for (int i = 1; i <= heap_size; ++i) 
	    result += "\n  (" + prio [i] + ", " + heap [i] + ")";
	return result + ")";
    }
}

