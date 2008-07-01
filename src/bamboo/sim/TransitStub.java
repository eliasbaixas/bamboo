/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.sim;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Vector;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.StreamTokenizer;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.Reader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import ostore.util.Carp;
import ostore.util.Pair;
import ostore.util.PriorityQueue;

/**
 * Code to use transit-stub graphs in the simulator.  Unlike
 * soss.network.TransitStub, we don't do any fancy shortest path algorithm
 * here.  Instead, we cache shortest path computations, on the assumption that
 * most nodes only talk to a small set of other nodes most of the time, which
 * should be true of Bamboo.  One advantage of doing things this way is that
 * simulation startup time is shorter, making this technique more appropriate
 * for short debugging cycles.  It might also be faster when the size of the
 * graph is much larger than the number of Bamboo instances.
 *
 * @author Sean C. Rhea
 * @version $Id: TransitStub.java,v 1.7 2004/01/16 23:13:33 srhea Exp $
 */
public class TransitStub extends CachingNetworkModel {

    protected static Logger logger = Logger.getLogger (TransitStub.class);
    static {
        // logger.setLevel (Level.DEBUG);
    }
    protected Node [] G;

    public static class Edge extends GraphUtils.Edge {
	public double inv_bw_s_per_byte;
        public final long latency_us () { return len; }
	public Edge (Node t, long l, double ib) {
            super (t, l);
	    inv_bw_s_per_byte = ib;
	}
    }

    public static class DomainId {
	public int dom, dnn, snum;
	public DomainId (int dom, int dnn, int snum) {
	    this.dom = dom;
	    this.dnn = dnn;
	    this.snum = snum;
	}
	public boolean equals (Object other) {
	    DomainId rhs = (DomainId) other;
	    if (dom != rhs.dom) return false;
	    if (snum != rhs.snum) return false;
	    if (snum == -1) return true;
	    return dnn == rhs.dnn;
	}
	public int hashCode () {
	    if (snum == -1) 
		return dom;
	    else 
		return (dom << 16) ^ (dnn << 8) ^ snum;
	}
    }

    public static class Node extends GraphUtils.Node {
	public DomainId domain;
	public Node (int n, DomainId d) { 
            super (n);
	    domain = d;
	}
    }

    public static final double INV_BW_1_Mbps = 1.0/1024/1024;

    public TransitStub (String filename) throws IOException {
	this (filename, 1000*1000, 
              1000, 5000, 10*1000, 10*1000, 
              INV_BW_1_Mbps/100.0, INV_BW_1_Mbps/1.5, 
              INV_BW_1_Mbps/45.0,  INV_BW_1_Mbps/1.5);
    }

    public TransitStub (String filename, int cache_size,
            long ss_lat, long st_lat, long tt_lat, long id_lat,
            double ss_beta, double st_beta, double tt_beta, double id_beta
            ) throws IOException {

        super (cache_size);

	FileInputStream is = new FileInputStream (filename);
	Reader reader = new BufferedReader (new InputStreamReader (is));
	StreamTokenizer tok = new StreamTokenizer (reader);

	System.out.println ("filename=" + filename);
	tok.resetSyntax ();
	tok.whitespaceChars ((int) ' ', (int) ' ');
	tok.wordChars ((int) '-', (int) '-');
	tok.wordChars ((int) '0', (int) '9');

	int lineno = 1;
	tok.nextToken ();
	while (tok.ttype == StreamTokenizer.TT_EOL) {
	    tok.nextToken ();
	    lineno++;
	}

	// Read in the node count.
	if (tok.ttype != StreamTokenizer.TT_WORD) {
	    System.err.println ("Expected node count on line " + 
		    lineno + ": " + tok);
	    System.exit (1); 
	}

	int n = Integer.parseInt (tok.sval);
	G = new Node [n];

	System.out.println ("Graph size = " + G.length);

	tok.nextToken ();
	while (tok.ttype == StreamTokenizer.TT_EOL) {
	    tok.nextToken ();
	    lineno++;
	}

	// Read in all the vertices.

	for (int vertex = 0; vertex < G.length; ++vertex) {
	    if (tok.ttype != StreamTokenizer.TT_WORD) {
		System.err.println ("Expected vertex number on line " + 
			lineno + ": " + tok);
		System.exit (1); 
	    }
	    if (Integer.parseInt (tok.sval) != vertex)
		Carp.die ("Expected vertex = " + vertex);

	    tok.nextToken ();
	    if (tok.ttype != StreamTokenizer.TT_WORD) {
		System.err.println ("Expected dom on line " + 
			lineno + ": " + tok);
		System.exit (1); 
	    }
	    int dom = Integer.parseInt (tok.sval);

	    tok.nextToken ();
	    if (tok.ttype != StreamTokenizer.TT_WORD) {
		System.err.println ("Expected dnn on line " + 
			lineno + ": " + tok);
		System.exit (1); 
	    }
	    int dnn = Integer.parseInt (tok.sval);

	    tok.nextToken ();
	    if (tok.ttype != StreamTokenizer.TT_WORD) {
		System.err.println ("Expected snum on line " + 
			lineno + ": " + tok);
		System.exit (1); 
	    }
	    int snum = Integer.parseInt (tok.sval);

	    tok.nextToken ();
	    if (tok.ttype != StreamTokenizer.TT_WORD) {
		System.err.println ("Expected snn on line " + 
			lineno + ": " + tok);
		System.exit (1); 
	    }
	    int snn = Integer.parseInt (tok.sval);

	    DomainId domain = new DomainId (dom, dnn, snum);
	    G [vertex] = new Node (vertex, domain);
	    
	    tok.nextToken ();
	    while (tok.ttype == StreamTokenizer.TT_EOL) {
		tok.nextToken ();
		lineno++;
	    }
	}

	// Read in all the edges.

	int edge_count = 0;
	LinkedList interdomain_edges = new LinkedList ();

	while (tok.ttype != StreamTokenizer.TT_EOF) {

	    if (tok.ttype != StreamTokenizer.TT_WORD) {
		System.err.println ("Expected from on line " + 
			lineno + ": " + tok);
		System.exit (1); 
	    }
	    int from = Integer.parseInt (tok.sval);

	    tok.nextToken ();
	    if (tok.ttype != StreamTokenizer.TT_WORD) {
		System.err.println ("Expected to on line " + 
			lineno + ".");
		System.exit (1); 
	    }
	    int to = Integer.parseInt (tok.sval);

	    tok.nextToken ();
	    if (tok.ttype != StreamTokenizer.TT_WORD) {
		System.err.println ("Expected length on line " + 
			lineno + ".");
		System.exit (1); 
	    }

	    edge_count += 1;

            long latency_us = 0;
	    double inv_bw_s_per_byte = 0.0;

	    Node src = G [from];
	    Node dst = G [to];
            boolean interdomain_edge = ! src.domain.equals (dst.domain);

	    if (src.domain.snum == -1) {
		// The source is in a transit domain.

		if (dst.domain.snum != -1) {
		    // The destination is in a stub domain.  This is a
		    // transit to stub edge.
                    latency_us = st_lat;
                    inv_bw_s_per_byte = st_beta;
		}
		else {
		    // The destination is in a transit domain.  This is a
		    // transit to transit edge.

                    latency_us = tt_lat;
                    inv_bw_s_per_byte = tt_beta;
		}
	    }
	    else {
		// The source is in a stub domain.

		if (dst.domain.snum == -1) {
		    // The destination is in a transit domain.  This is a
		    // stub to transit edge.

                    latency_us = st_lat;
                    inv_bw_s_per_byte = st_beta;
		}
		else {
		    // The destination is in a stub domain.  This is a stub
		    // to stub edge.

		    if (interdomain_edge) {
			// They're in different stub domains.
                        latency_us = id_lat;
                        inv_bw_s_per_byte = id_beta;
		    }
		    else {
			// They're in the same stub domain.
                        latency_us = ss_lat;
                        inv_bw_s_per_byte = ss_beta;
		    }
		}
	    }

            src.add_edge (new Edge (dst, latency_us, inv_bw_s_per_byte));
            dst.add_edge (new Edge (src, latency_us, inv_bw_s_per_byte));

	    tok.nextToken ();
	    while (tok.ttype == StreamTokenizer.TT_EOL) {
		tok.nextToken ();
		lineno++;
	    }
	}

	Carp.out ("Edges = " + edge_count);

	is.close ();

	// Create a new graph, converting each interdomain edge to a node
	// with two edges only.
    }

    protected NetworkModel.RouteInfo 
        the_real_compute_route_info (int src, int dst) {

        if (src == dst)
	    return new NetworkModel.RouteInfo (0, 0.0);

        Node [] pred = new Node [G.length];
        long [] dist = new long [G.length];
        GraphUtils.dijkstra (G, pred, dist, G [src], G [dst]);

        Node walker = G [dst];
        long total_lat = 0;
        double max_inv_bw = 0.0;

        while (! walker.equals (G [src])) {
            if (logger.isDebugEnabled ()) {
                logger.debug ("walker=" + walker + 
                        ", pred=" + pred [walker.number]);
            }
            Edge e = (Edge) walker.get_edge_to (pred [walker.number]);
            total_lat += e.latency_us ();
            if (e.inv_bw_s_per_byte > max_inv_bw)
                max_inv_bw = e.inv_bw_s_per_byte;
            if (logger.isDebugEnabled ()) {
                logger.debug ("lat=" + e.latency_us () + 
                        ", bw=" + (1.0/e.inv_bw_s_per_byte));
            }
            walker = pred [walker.number];
        }

        return new NetworkModel.RouteInfo (total_lat, max_inv_bw);
    }
}

