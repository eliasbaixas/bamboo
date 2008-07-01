/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.sim;
import bamboo.lss.PriorityQueue;
import java.util.HashSet;
import java.util.Set;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * A simple implementation of Dijkstra's shortest-path algorithm.
 *
 * @author Sean C. Rhea
 * @version $Id: GraphUtils.java,v 1.2 2003/12/20 22:49:25 srhea Exp $
 */
public class GraphUtils {

    protected static Logger logger = Logger.getLogger (GraphUtils.class);
    static {
        // logger.setLevel (Level.DEBUG);
    }

    public static class Edge {
	public Node tail;
	public long len;
	public Edge next;

	public Edge (Node t, long l) {
            tail = t; len = l;
	}
    }

    public static class Node {
	public int number;
	public Edge edges;

	public Node (int n) { number = n; }

	public final void add_edge (Edge e) { 
	    e.next = edges; edges = e;
	}

        public final Edge get_edge_to (Node tail) {
            Edge walker = edges;
            while ((walker != null) && (! walker.tail.equals (tail)))
                walker = walker.next;
            return walker;
        }

	public boolean equals (Object other) {
	    return ((Node) other).number == number;
	}

	public int hashCode () { return number; }

        public String toString () { return "(Node " + number + ")"; }
    }

    /**
     * Compute the shortest paths from <code>src</code> to <code>dst</code>,
     * or if <code>dst==null</code>, to all nodes in <code>G</code>.
     */
    public static final void dijkstra (
	    Node [] G, Node [] pred, long [] dist, Node src, Node dst) {

        Set relaxed = new HashSet ();
	PriorityQueue Q = new PriorityQueue (G.length);
	for (int i = 0; i < G.length; ++i) {
	    Node v = G [i];
	    if (v.equals (src))
		dist [v.number] = 0;
	    else
		dist [v.number] = Long.MAX_VALUE;
	    pred [v.number] = null;
	}

	Q.add (src, 0);

	while (! Q.isEmpty ()) {
            Node u = (Node) Q.removeFirst ();

            if (u.equals (dst)) {
                logger.debug (u + "is relaxed");
                return;
            }

            if (! relaxed.contains (u)) {
                logger.debug ("relaxing " + u);
                for (Edge e = u.edges; e != null; e = e.next) {
                    Node v = e.tail;
                    logger.debug (v + " old_dist=" + dist [v.number] + 
                            " new_dist=" + (dist [u.number] + e.len));
                    if (dist [v.number] > dist [u.number] + e.len) {
                        dist [v.number] = dist [u.number] + e.len;
                        pred [v.number] = u;
                        Q.add (v, dist [v.number]);
                    }
                }
                relaxed.add (u);
            }
        }
    }
}

