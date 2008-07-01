/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.vis;

import bamboo.router.LeafSet;
import bamboo.router.NeighborInfo;
import bamboo.router.RoutingTable;
import bamboo.util.GuidTools;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import javax.swing.*;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import ostore.util.NodeId;
import static bamboo.util.Curry.*;
import bamboo.vis.Vis.BambooNode;
import bamboo.vis.FetchNodeInfoThread.NodeInfo;
import bamboo.vis.ExtendedNeighborInfo;

/**
 * Print all the leaf set latencies.
 *
 * @author Sean C. Rhea
 * @version $Id: LeafSetLatencies.java,v 1.5 2005/08/23 02:41:22 cgtime Exp $
 */
public class LeafSetLatencies {
    
    protected static HashSet already_probed = new HashSet ();
    protected static String nid2url (NodeId n) {
        return "http://" + n.address ().getHostAddress () + ":" +
            (n.port () + 1) + "/";
    }
    /*
    public static FetchNodeInfoThread.FetchDoneCb fetch_done_cb = 
        new FetchNodeInfoThread.FetchDoneCb () {
	    public void fetch_done (
                                    final String url, final FetchNodeInfoThread.NodeInfo ninfo,
                                    final boolean success, final Object user_data) {
                
                if (! success) 
                    return;
    */
    public static Thunk1<BambooNode> FetchDoneCb = new Thunk1<BambooNode> () {
        public void run (BambooNode node) {
            NodeInfo ninfo = node.ninfo;
            for (int j = 0; j < 2; ++j) {
                Iterator i = ((j == 0) ? ninfo.preds : ninfo.succs).iterator ();
                while (i.hasNext ()) {
                    ExtendedNeighborInfo ni = (ExtendedNeighborInfo) i.next ();
                    logger.info (ninfo.addr + " " + ni.node_id + " " + ni.rtt_ms);
                    synchronized (already_probed) {
                        if (! already_probed.contains (ni.node_id)) {
                            already_probed.add (ni.node_id);
                            node.FNIT.add_work(nid2url(ni.node_id),null);
                        }
                    }
                }
            }
        }
    };
    
    public static Logger logger = Logger.getLogger (LeafSetLatencies.class);
    
    public static void main (String [] args) throws Exception {
        PatternLayout pl = new PatternLayout ("%d{ISO8601} %m\n");
        ConsoleAppender ca = new ConsoleAppender (pl);
        Logger.getRoot ().addAppender (ca);
        Logger.getRoot ().setLevel (Level.INFO);
        final FetchNodeInfoThread fetcher = new FetchNodeInfoThread (FetchDoneCb, null);
        for (int i = 0; i < args.length; ++i) {
            NodeId id = new NodeId (args [i]);
            already_probed.add (id);
            fetcher.add_work (nid2url(id), null);
        }
        fetcher.start();
        /*
        final FetchNodeInfoThread [] fetchers = new FetchNodeInfoThread [20];
        for (int i = 0; i < fetchers.length; ++i) {
            fetchers [i] = new FetchNodeInfoThread (fetch_done_cb);
            fetchers [i].start ();
        }
        */
        Thread check_done = new Thread () {
                public void run () {
                    while (true) {
			boolean all_idle = true;
			//for (int i = 0; i < fetchers.length; ++i) {
                        if (! fetcher.idle ())
                            all_idle = false;
                        //}
			if (all_idle) 
                            System.exit (0);
			else {
                            try { sleep (1000); } catch (Exception e) {}
			}
                    }
                }
	    };
        check_done.start ();
    }
}

