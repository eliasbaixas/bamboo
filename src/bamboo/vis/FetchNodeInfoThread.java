/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.vis;

import bamboo.lss.ASyncCore;
import bamboo.router.LeafSet;
import bamboo.router.RoutingTable;
import bamboo.util.GuidTools;
import java.awt.*;
import java.awt.event.*;
import java.awt.geom.*;
import java.io.*;
import java.math.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;
import java.util.regex.*;
import javax.swing.*;
import javax.swing.event.*;
import org.apache.log4j.Logger;
import ostore.util.NodeId;
import static bamboo.util.Curry.*;
import bamboo.vis.Vis.BambooNode;
import static java.nio.channels.SelectionKey.*;

public class FetchNodeInfoThread extends Thread {
    
    public static Logger logger = Logger.getLogger (FetchNodeInfoThread.class);
    public static float biggestStorageFound = 0;
    public static long biggestUptimeFound = 0;
    protected BigInteger MODULUS = BigInteger.valueOf (2).pow (160);
    
    protected LinkedList<String> waitlist = new LinkedList<String> ();
    protected HashMap<String,NodeInfo> waiting = 
        new HashMap<String,NodeInfo> ();
    protected HashMap<String,NodeInfo> inflight = 
        new HashMap<String,NodeInfo>();
    
    public HashMap<BigInteger,BambooNode> nodes_by_id = 
        new HashMap<BigInteger,BambooNode> ();
    
    protected ASyncCore acore;
    protected Vis stub_vis;
    
    protected Thunk1<BambooNode> fetch_succeed;
    protected Thunk1<BambooNode> fetch_fail;

    protected long max_concurrent = 100;
    protected long check_period_ms = 5*60*1000;

    private final int MATCHING_NODE = 0;
    private final int NOT_MATCHING_NODE = 1;
    private final int INCORRECT_INPUT = 2;

    private final int TYPE_NOTHING = 0;
    private final int TYPE_LITERAL = 1;
    private final int TYPE_NODE_VALUE_FLOAT = 2;
    private final int TYPE_NODE_VALUE_STRING = 3;

    public final String [] GATEWAY_NODES_IP =
    {
        "http://12.46.129.21:5851/",
        "http://128.208.4.197:5851/",
        "http://169.229.50.14:5851/",
        "http://152.3.138.1:5851/",
        "http://128.223.6.111:5851/",
        "http://128.83.143.152:5851/",
        "http://141.213.4.201:5851/",
        "http://132.239.17.224:5851/"
    };
    
    public static class NodeInfo {
        public NodeId addr;
        public BigInteger id;
        public String url;
        public double [] coordinates;
        public float currentStorage;
        public long uptime_number;
        public String uptime_string;
        public String ID;
        public String IP;
        public String hostname;
        public int build;
        public int port;
        public int estimate;
        public LinkedList preds = new LinkedList ();
        public LinkedList succs = new LinkedList ();
        public LinkedList rt = new LinkedList ();
    }
    
    protected static BigInteger mult = BigInteger.valueOf (2).pow (160-32);
    
    protected static LinkedList work_queue = new LinkedList ();
    protected static int thread_count = 0;
    protected boolean idle = true;
    
    public boolean idle () { synchronized (work_queue) { return idle; } }
    
    public FetchNodeInfoThread (Thunk1<BambooNode> fetch_succeed, Thunk1<BambooNode> fetch_fail) throws IOException {
        this.fetch_succeed = fetch_succeed;
        this.fetch_fail = fetch_fail;
        this.acore = new bamboo.lss.ASyncCoreImpl ();
        stub_vis = new Vis ();
    }

    public FetchNodeInfoThread (Thunk1<BambooNode> fetch_succeed, Thunk1<BambooNode> fetch_fail, Vis stub_vis) throws IOException {
        this.fetch_succeed = fetch_succeed;
        this.fetch_fail = fetch_fail;
        this.acore = new bamboo.lss.ASyncCoreImpl ();
        this.stub_vis = stub_vis;
    }
    
    public static void add_work (String url, Object user_data) {
        synchronized (work_queue) {
            work_queue.addLast (new Object [] {url, user_data});
            if (work_queue.size () == 1)
                work_queue.notify ();
        }
    }
    
    public Object [] get_work () {
        Object [] pair = null;
        synchronized (work_queue) {
            idle = true;
            while (work_queue.isEmpty ()) {
                try { work_queue.wait (); }
                catch (InterruptedException e) {} 
            }
            pair = (Object []) work_queue.removeFirst ();
            idle = false;
        }
        return pair;
    }
    
    public void run () {
        if (work_queue.size () == 0) {
            for (int x = 0; x < GATEWAY_NODES_IP.length; x++) {
                add_work (GATEWAY_NODES_IP [x], null);
            }
        }
        
        acore.register_timer (0, check_all_nodes_cb);
        Thread t = new Thread ("AsyncCore Thread") {
                public void run () {
                    acore.async_main ();
                }
	    };
        t.start ();
    }
    
    protected static Pattern hostname = Pattern.compile (
                                                         ".*Hostname:.*<td>([^<]+)</td>.*");
    
    protected static Pattern ip_addr = Pattern.compile (
                                                        ".*IP Address:.*<td>([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+)</td>.*");
    //"<tr><td><em>IP Address:</em></td><td></td><td>([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+)</td>");
    
    protected static Pattern port = Pattern.compile (
                                                     ".*Port:.*<td>([0-9]+)</td>.*");
    
    protected static Pattern id = Pattern.compile (
                                                   ".*ID:.*<td>0x([0-9a-f]+)</td>.*");
    
    protected static Pattern uptime = Pattern.compile (
                                                       ".*Uptime:.*<td>" + 
                                                       "([0-9]*) (weeks|week|days|day|hours|hour|minutes|minute|seconds|second)" +
                                                       "(?: ([0-9]*) (days|day|hours|hour|minutes|minute|seconds|second))?" +
                                                       "(?: ([0-9]*) (hours|hour|minutes|minute|seconds|second))?" +
                                                       "(?: ([0-9]*) (minutes|minute|seconds|second))?" +
                                                       "(?: ([0-9]*) (seconds|second))?" + 
                                                       "</td>.*");
    
    protected static Pattern coord = Pattern.compile (
                                                      ".*Virtual Coordinates:.*<td>(-?[0-9.]+), "
                                                      + "(-?[0-9.]+), (-?[0-9.]+)</td>.*");
    
    protected static Pattern currentStorage = Pattern.compile (
                                                               ".*Current Storage:.*<td>([0-9.]+) MBs</td>.*");
    
    protected static Pattern build = Pattern.compile (
                                                      ".*Build:.*<td>([0-9]+)</td>.*");
    
    protected static Pattern estimate = Pattern.compile (
                                                         ".*Estimated Network Size:.*<td>([0-9]+) nodes</td>.*");
    
    protected static Pattern ls_neighbor = Pattern.compile (
                                                            ".*<tr><td align=\"center\">(-?[0-9]+)</td><td align=\"center\">" + 
                                                            "<a href=\"[^\"]*\">([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+)</a></td>" +
                                                            "<td align=\"center\">([0-9]+)</td><td align=\"center\">" + 
                                                            "0x([0-9a-f]+)</td><td align=\"center\">([0-9]+)</td></tr>.*");
    
    protected static Pattern rt_neighbor = Pattern.compile (
                                                            ".*<tr><td align=\"center\">([0-9]+)</td><td align=\"center\">" + 
                                                            "([0-9]+)</td><td align=\"center\"><a href=\"[^\"]*\">" + 
                                                            "([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+)</a></td><td " + 
                                                            "align=\"center\">([0-9]+)</td><td align=\"center\">0x" + 
                                                            "([0-9a-f]+)</td><td align=\"center\">([0-9]+)</td></tr>.*");
    
    public static NodeInfo parse_body (InputStream is) throws IOException {
        NodeInfo result = new NodeInfo ();
        InetAddress result_addr = null;
        int result_port = -1;
        String result_hostname = null;
        BufferedReader reader = new BufferedReader (new InputStreamReader (is));
        String line = null;
        int total_rtt_ms = 0;
        int total_neighbors = 0;
        String result_uptime = null;
        boolean result_addr_set = false;
        while ((line = reader.readLine ()) != null) {
            Matcher m = null;
            m = hostname.matcher (line);
            if (m.matches ()) {
                result_hostname = m.group (1);
                result.hostname = m.group (1);
            }
            m = ip_addr.matcher (line);
            if (m.matches ()) {
                result_addr = InetAddress.getByName (m.group (1));
                result_addr_set = true;
                result.IP = m.group (1);
            }
            m = port.matcher (line);
            if (m.matches ()) {
                result_port = Integer.parseInt (m.group (1));
                result.port = result_port;
            }
            m = id.matcher (line);
            if (m.matches ()) {
                result.id = new BigInteger (m.group (1), 16).multiply (mult);
                result.ID = m.group (1);
            }
            m = build.matcher (line);
            if (m.matches ()) {
                result.build = Integer.parseInt (m.group (1));
            }
            m = estimate.matcher (line);
            if (m.matches ()) {
                result.estimate = Integer.parseInt (m.group (1));
            }
            m = uptime.matcher (line);
            if (m.matches ()) {
                String week_string = "week";
                String day_string = "day";
                String hour_string = "hour";
                String minute_string = "minute";
                String second_string = "second";
                
                int weeks = 0;
                int days = 0;
                int hours = 0;
                int minutes = 0;
                int seconds = 0;
                
                result.uptime_string = "";
                int index = 1;
                while (index < m.groupCount ()) {
                    if (m.group (index + 1) == null) {
                        break;
                    }
                    else if (week_string.regionMatches (0, m.group (index + 1), 0, week_string.length ())) {
                        weeks = Integer.parseInt (m.group (index));
                    }
                    else if (day_string.regionMatches (0, m.group (index + 1), 0, day_string.length ())) {
                        days = Integer.parseInt (m.group (index));
                    }
                    else if (hour_string.regionMatches (0, m.group (index + 1), 0, hour_string.length ())) {
                        hours = Integer.parseInt (m.group (index));
                    }
                    else if (minute_string.regionMatches (0, m.group (index + 1), 0, minute_string.length ())) {
                        minutes = Integer.parseInt (m.group (index));
                    }
                    else if (second_string.regionMatches (0, m.group (index + 1), 0, second_string.length ())) {
                        seconds = Integer.parseInt (m.group (index));
                    }
                    else {
                        //This else should never be hit unless there is some time increment
                        //we don't test for that we otherwise should.
                        break;
                    }
                    
                    result.uptime_string = result.uptime_string + m.group (index) + " " + m.group (index + 1) + " ";
                    index += 2;
                }
                
                result.uptime_number = weeks*604800 + days*86400 + hours*3600 + minutes*60 + seconds;
                if(result.uptime_number > biggestUptimeFound) {
                    biggestUptimeFound = result.uptime_number;
                }
                result_uptime = result.uptime_string;
            }
            m = coord.matcher (line);
            if (m.matches ()) {
                result.coordinates = new double [3];
                result.coordinates [0] = Double.parseDouble (m.group (1));
                result.coordinates [1] = Double.parseDouble (m.group (2));
                result.coordinates [2] = Double.parseDouble (m.group (3));
            }
	    m = currentStorage.matcher (line);
	    if (m.matches ()) {
                result.currentStorage = Float.parseFloat (m.group(1));
                if(result.currentStorage > biggestStorageFound) {
                    biggestStorageFound = result.currentStorage;
                }
	    }
            m = ls_neighbor.matcher (line);
            if (m.matches ()) {
                int position = Integer.parseInt (m.group (1));
                InetAddress addr = InetAddress.getByName (m.group (2));
                int port = Integer.parseInt (m.group (3));
                BigInteger id = new BigInteger(m.group (4), 16).multiply (mult);
                int rtt_ms = Integer.parseInt (m.group (5));
                NodeId n = new NodeId (port, addr);
                ExtendedNeighborInfo neigh = new ExtendedNeighborInfo (n, id, rtt_ms);
                if (position < 0) 
                    result.preds.addFirst (neigh);
                else 
                    result.succs.addLast (neigh);
                total_rtt_ms += rtt_ms;
                ++total_neighbors;
            }
            m = rt_neighbor.matcher (line);
            if (m.matches ()) {
                int level = Integer.parseInt (m.group (1));
                int digit = Integer.parseInt (m.group (2));
                InetAddress addr = InetAddress.getByName (m.group (3));
                int port = Integer.parseInt (m.group (4));
                BigInteger id = new BigInteger(m.group (5), 16).multiply (mult);
                int rtt_ms = Integer.parseInt (m.group (6));
                NodeId n = new NodeId (port, addr);
                ExtendedNeighborInfo neigh = new ExtendedNeighborInfo (n, id, rtt_ms);
                result.rt.addLast (neigh);
                total_rtt_ms += rtt_ms;
                ++total_neighbors;
            }
        }
        if (result_addr == null) {
            return null;
        }
        // Avoid both the normal and reverse DNS lookup.
        result_addr = InetAddress.getByAddress (
                                                result_hostname, result_addr.getAddress ());
        result.addr = new NodeId (result_port, result_addr);
        double avg_rtt_ms = (((double) total_rtt_ms) / total_neighbors);
        if (avg_rtt_ms > 3000.0) {
            logger.warn (result_hostname + " has average RTT of " + 
			 avg_rtt_ms + " ms.  Uptime is " + result_uptime);
        }
        return result;
    }
    
    protected String nid2url (NodeId n) {
        return "http://" + n.address ().getHostAddress () + ":" +
            (n.port () + 1) + "/";
    }
    
    protected static InetSocketAddress url2addr (String str) {
        try { 
            URL url = new URL (str); 
            return new InetSocketAddress (url.getHost (), url.getPort ());
        }
        catch (MalformedURLException e) { return null; }
    }
    
    protected void fetch_succeeded (final NodeInfo ninfo) {
        final FetchNodeInfoThread FNIT = this;
        logger.debug ("fetch_succeeded " + ninfo.url);
        synchronized (inflight) {
            inflight.remove (ninfo.url);
        }
        java.awt.EventQueue.invokeLater (new Runnable () {
                public void run () {
                    assert ninfo.id != null;
                    BambooNode node = nodes_by_id.get (ninfo.id);
                    if (node == null) {
			node = stub_vis.return_new_node (ninfo.id, ninfo.addr, ninfo.coordinates);
			nodes_by_id.put (ninfo.id, node);
                    }
                    node.set_coordinates (ninfo.coordinates);
                    node.last_check_ms = System.currentTimeMillis ();
                    node.uptime_number = ninfo.uptime_number;
                    node.uptime_string = ninfo.uptime_string;
                    node.current_storage = ninfo.currentStorage;
                    node.ID = ninfo.ID;
                    node.IP = ninfo.IP;
                    node.build = ninfo.build;
                    node.estimate = ninfo.estimate;
                    node.port = ninfo.port;
                    node.hostname = ninfo.hostname;
                    node.ninfo = ninfo;
                    node.FNIT = FNIT;

                    ExtendedNeighborInfo ni = new ExtendedNeighborInfo (ninfo.addr, ninfo.id);
                    node.leaf_set = new LeafSet (
                                                 ni, Math.max(ninfo.preds.size (), ninfo.succs.size ()),
                                                 MODULUS);
                    node.ls_lat = new HashMap<ExtendedNeighborInfo,Long>();
                    Iterator i = ninfo.preds.iterator ();
                    while (i.hasNext ()) {
			ExtendedNeighborInfo n = (ExtendedNeighborInfo) i.next ();
                        node.leaf_set.add_node (n);
                        node.ls_lat.put(n, new Long(n.rtt_ms));
                        String nurl = nid2url (n.node_id);
			if ((! nodes_by_id.containsKey (n.guid)) &&
                            (! outstanding (nurl))) {
                            add_work (nurl, n.guid);
			}
                    }
                    i = ninfo.succs.iterator ();
                    while (i.hasNext ()) {
			ExtendedNeighborInfo n = (ExtendedNeighborInfo) i.next ();
			node.leaf_set.add_node (n);
                        node.ls_lat.put(n, new Long(n.rtt_ms));
			String nurl = nid2url (n.node_id);
			if ((! nodes_by_id.containsKey (n.guid)) &&
                            (! outstanding (nurl))) {
                            add_work (nurl, n.guid);
			}
                    }
                    
                                        
                    i = ninfo.rt.iterator ();
                    node.routing_table = new RoutingTable(ni, 1.0, MODULUS, 160, 2);
                    int j = 0;
                    while (i.hasNext ()) {
			ExtendedNeighborInfo n = (ExtendedNeighborInfo) i.next ();
			node.routing_table.force_add (new ExtendedNeighborInfo (n.node_id, n.guid), n.rtt_ms);
			String nurl = nid2url (n.node_id);
			if ((! nodes_by_id.containsKey (n.guid)) &&
                            (! outstanding (nurl))) {
                            add_work (nurl, n.guid);
			}
                    }
                    
                    if (fetch_succeed != null) {
                        acore.register_timer (0, curry(fetch_succeed, node));
                    }
                }
	    });
    }
    
    protected void fetch_failed (final NodeInfo ninfo) {
        logger.debug ("fetch_failed " + ninfo.url);
        synchronized (inflight) {
            inflight.remove (ninfo.url);
        }
        if (ninfo.id != null) {
            java.awt.EventQueue.invokeLater (new Runnable () {
                    public void run () { 
			BambooNode node = nodes_by_id.get (ninfo.id);
			if (node != null) {
                            nodes_by_id.remove (ninfo.id);
			}
                        //The user must be careful that the Thunk fetch_fail can handle
                        //being passed a null node
                        if (fetch_fail != null) {
                            acore.register_timer (0, curry(fetch_fail, node));
                        }
                    }
                });
        }
    }

    public Iterator get_all_nodes () {
        LinkedList list = new LinkedList ();
        
        for (BigInteger id : nodes_by_id.keySet ()) {
	    list.add((BambooNode) nodes_by_id.get (id));
        }
        
        return list.iterator ();
    }
    
    protected Runnable check_all_nodes_cb = new Runnable () {
            public void run() {
                java.awt.EventQueue.invokeLater (new Runnable () {
                        public void run () { 
                            long now_ms = System.currentTimeMillis ();
                            for (BigInteger id : nodes_by_id.keySet ()) {
                                BambooNode node = nodes_by_id.get (id);
                                String nurl = nid2url (node.node_id);
                                if ((! outstanding (nurl)) && 
                                    (now_ms - node.last_check_ms > check_period_ms)) {
				    add_work (nurl, id);
                                }
                            }
                        }
                    });
                acore.register_timer (10*1000, check_all_nodes_cb);
            }
        };
    
    protected boolean outstanding (String url) {
        synchronized (inflight) {
            return waiting.containsKey (url) || inflight.containsKey (url);
        }
    }
    
    protected void add_work (String url, BigInteger id) {
        if (! outstanding (url)) {
            NodeInfo ninfo = new NodeInfo ();
            ninfo.url = url;
            ninfo.id = id;
            waiting.put (url, ninfo);
            waitlist.addLast (url);
            check_waiting ();
        }
    }
    
    protected void check_waiting () {
        LinkedList<NodeInfo> todo = new LinkedList<NodeInfo> ();
        synchronized (inflight) {
            while ((! waiting.isEmpty ()) && 
                   (inflight.size () < max_concurrent)) {
                String url = waitlist.removeFirst ();
                NodeInfo ninfo = waiting.remove (url);
                inflight.put (ninfo.url, ninfo);
                todo.add (ninfo);
            }
        }
        for (NodeInfo ninfo : todo) {
	    acore.register_timer (0, curry(fetch_node_info, ninfo));
        }
    }
    
    protected Thunk1<NodeInfo> fetch_node_info = 
        new Thunk1<NodeInfo> () {
	    public void run (NodeInfo ninfo) {
                SocketChannel channel = null;
                try {
                    InetSocketAddress addr = url2addr (ninfo.url);
                    if (addr == null) {
                        fetch_failed (ninfo);
                        return;
                    }
                    channel = SocketChannel.open();
                    channel.configureBlocking(false);
                    channel.connect(addr);
                    
                    acore.register_selectable (channel, SelectionKey.OP_CONNECT,
                                               curry(connect_cb, ninfo, channel));
                }
                catch (IOException e) { assert false : e; } 
	    }
        };
    
    protected Thunk2<NodeInfo,SocketChannel> connect_cb = 
        new Thunk2<NodeInfo,SocketChannel> () {
	    public void run(NodeInfo ninfo, SocketChannel channel) {
                try {
                    if (channel.finishConnect ()) {
                        acore.unregister_selectable (channel, OP_CONNECT);
                        acore.register_selectable (channel, OP_WRITE,
                                                   curry(write_cb, ninfo, channel));
                    }	
                }
                catch (IOException e) { 
                    acore.unregister_selectable (channel);
                    try { channel.close (); } catch (IOException ignore) {}
                    fetch_failed (ninfo);
                }
	    }
        };
    
    protected Thunk2<NodeInfo,SocketChannel> write_cb = 
        new Thunk2<NodeInfo,SocketChannel> () {
	    public void run(NodeInfo ninfo, SocketChannel channel) {
                String getstr = "GET / HTTP/1.0\r\n\r\n";
                ByteBuffer pkt = ByteBuffer.wrap (getstr.getBytes ()); 
                try { 
                    channel.write (pkt); 
                    if (pkt.position () == pkt.limit ()) {
                        ByteBuffer resp = ByteBuffer.allocate (32*1024);
                        acore.unregister_selectable (channel, OP_WRITE);
                        acore.register_selectable (channel, OP_READ,
                                                   curry(read_cb, ninfo, channel, resp));
                    }
                } 
                catch (IOException e) { 
                    acore.unregister_selectable (channel);
                    try { channel.close (); } catch (IOException ignore) {}
                    fetch_failed (ninfo);
                    return;
                }
	    }
        };
    
    protected Thunk3<NodeInfo,SocketChannel,ByteBuffer> read_cb = 
        new Thunk3<NodeInfo,SocketChannel,ByteBuffer> () {
	    public void run(NodeInfo ninfo, SocketChannel channel, 
			    ByteBuffer read_buf) {
                                while (true) {
                                    int n = 0;
                                    try { n = channel.read (read_buf); }
                                    catch (IOException e) { 
                                        acore.unregister_selectable (channel);
                                        try { channel.close (); } catch (IOException ignore) {}
                                        fetch_failed (ninfo);
                                        return;
                                    }
                                    if (n == 0) 
                                    return;
                                    if (n < 0)
                                    break;
                                }
                                acore.unregister_selectable (channel);
                                try { channel.close (); } catch (IOException e) {}
                                NodeInfo new_ninfo = null;
                                ByteArrayInputStream is = new ByteArrayInputStream (
                                                                                    read_buf.array (), read_buf.arrayOffset (), 
                                                                                    read_buf.position ());
                                try { new_ninfo = parse_body (is); }
                                catch (IOException e) { assert false : e; }
                                if (new_ninfo == null) {
                                    logger.warn ("could not parse body from " + ninfo);
                                    fetch_failed (ninfo);
                                    return;
                                }
                                assert new_ninfo.id != null : new String (
                                                                          read_buf.array (), read_buf.arrayOffset (), 
                                                                          read_buf.position ());
                                new_ninfo.url = ninfo.url;
                                fetch_succeeded (new_ninfo);
			    }
        };

    public BambooNode find_node (String s) {
        BambooNode current_node;
        Iterator IT = get_all_nodes ();
        s = s.trim ();

        while (IT.hasNext ()) {
            current_node = (BambooNode) IT.next ();

            if (s != null && 
               (s.equals (current_node.hostname) ||
                s.equals (current_node.ID) ||
                s.equals ("0x" + current_node.ID) ||
                s.equals (current_node.IP))) {
                return current_node;
            }
        }
        return null;
    }

    //Test the LinkList returned by find_nodes to see if the string was malformed.
    public static boolean test_bad_find_nodes_input (LinkedList L) {
        if (L == null) {
            return false;
        }
        if (L.size () == 1) {
            BambooNode b = (BambooNode) L.getFirst ();

            if (b.guid == null && b.node_id == null && b.coordinates == null) {
                return true;
            }
        }
        return false; 
    }

    //This function will return a LinkedList of matching nodes for the specified string.
    //It will return a specially crafted LinkedList called bad_input if the string is malformed.
    //You can test for this with the above static method test_bad_find_nodes_input.
    //To know how the string needs to be formatted, read the help in the visualizer.
    public LinkedList find_nodes (String s) {
        LinkedList bad_input = new LinkedList ();
        bad_input.addLast (stub_vis.return_new_node (null, null, null));

        if (s == null) {
            return bad_input;
        }
        
        LinkedList tokens = new LinkedList ();
        if (!tokenize (s, tokens)) {
            return bad_input;
        }
        
        BambooNode current_node;
        LinkedList matching_nodes = new LinkedList ();
        Iterator IT = get_all_nodes ();
        
        while (IT.hasNext ()) {
            current_node = (BambooNode) IT.next ();
            
            int node_result = interpret_tokens (0, tokens, current_node);
            if (node_result == MATCHING_NODE) {
                matching_nodes.addLast (current_node);
            }
            else if (node_result == INCORRECT_INPUT) {
                return bad_input;
            }
        }
        return matching_nodes;
    }

    protected boolean tokenize (String input_string, LinkedList tokens) {
        //First parse all runs of non-whitespace characters
        int index_first = -1;
        char input_chars [] = input_string.toCharArray ();

        for (int index = 0; index < input_string.length (); index++) {
            char current_char = input_chars [index];
            if (current_char == ' ' || 
                current_char == '\n' || 
                current_char == '\t') {
                if (index_first != -1) {
                    char [] new_token = new char [index - index_first];
                    input_string.getChars (index_first, index, new_token, 0);
                    tokens.addLast (new_token);
                }
                index_first = -1;
            }
            else {
                if (index_first == -1) {
                    index_first = index;
                }
            }
        }

        if (index_first != -1) {
            char [] new_token = new char [input_string.length () - index_first];
            input_string.getChars(index_first, input_string.length (), new_token, 0);
            tokens.addLast (new_token);
        }

        if (tokens.size () < 2) {
            return false;
        }

        return true;
    }

    protected int interpret_tokens (int index, LinkedList tokens, BambooNode node) {
        String token1, token2, token3, token4;
        double token1_float = Double.NaN;
        double token3_float = Double.NaN;
        int this_result;
        int token1_type = TYPE_NOTHING;
        int token3_type = TYPE_NOTHING;

        if (!check_bounds (index+2, tokens)) {
            return INCORRECT_INPUT;
        }

        token1 = String.valueOf ((char []) tokens.get (index));
        token2 = String.valueOf ((char []) tokens.get (index+1));
        token3 = String.valueOf ((char []) tokens.get (index+2));
        index += 3;

        if (token1.equalsIgnoreCase ("build")) {
            token1_type = TYPE_NODE_VALUE_FLOAT;
            token1_float = Double.valueOf (String.valueOf (node.build));
        }
        else if (token1.equalsIgnoreCase ("hostname")) {
            token1_type = TYPE_NODE_VALUE_STRING;
            token1 = node.hostname;
        }
        else if (token1.equalsIgnoreCase ("ip")) {
            token1_type = TYPE_NODE_VALUE_STRING;
            token1 = node.IP;
        }
        else if (token1.equalsIgnoreCase ("port")) {
            token1_type = TYPE_NODE_VALUE_FLOAT;
            token1_float = Double.valueOf (String.valueOf (node.port));
        }
        else if (token1.equalsIgnoreCase ("id")) {
            token1_type = TYPE_NODE_VALUE_STRING;
            token1 = node.ID;
        }
        else if (token1.equalsIgnoreCase ("uptime")) {
            token1_type = TYPE_NODE_VALUE_FLOAT;
            token1_float = Double.valueOf (String.valueOf (node.uptime_number));
        }
        else if (token1.equalsIgnoreCase ("storage")) {
            token1_type = TYPE_NODE_VALUE_FLOAT;
            token1_float = Double.valueOf (String.valueOf (node.current_storage));
        }
        else if (token1.equalsIgnoreCase ("x_coord")) {
            token1_type = TYPE_NODE_VALUE_FLOAT;
            token1_float = ((node.coordinates == null) ? 0.0 : node.coordinates [0]);
        }
        else if (token1.equalsIgnoreCase ("y_coord")) {
            token1_type = TYPE_NODE_VALUE_FLOAT;
            token1_float = ((node.coordinates == null) ? 0.0 : node.coordinates [1]);
        }
        else if (token1.equalsIgnoreCase ("estimate")) {
            token1_type = TYPE_NODE_VALUE_FLOAT;
            token1_float = Double.valueOf (String.valueOf (node.estimate));
        }
        else {
            token1_type = TYPE_LITERAL;
        }


        if (token3.equalsIgnoreCase ("build")) {
            token3_type = TYPE_NODE_VALUE_FLOAT;
            token3_float = Double.valueOf (String.valueOf (node.build));
        }
        else if (token3.equalsIgnoreCase ("hostname")) {
            token3_type = TYPE_NODE_VALUE_STRING;
            token3 = node.hostname;
        }
        else if (token3.equalsIgnoreCase ("ip")) {
            token3_type = TYPE_NODE_VALUE_STRING;
            token3 = node.IP;
        }
        else if (token3.equalsIgnoreCase ("port")) {
            token3_type = TYPE_NODE_VALUE_FLOAT;
            token3_float = Double.valueOf (String.valueOf (node.port));
        }
        else if (token3.equalsIgnoreCase ("id")) {
            token3_type = TYPE_NODE_VALUE_STRING;
            token3 = node.ID;
        }
        else if (token3.equalsIgnoreCase ("uptime")) {
            token3_type = TYPE_NODE_VALUE_FLOAT;
            token3_float = Double.valueOf (String.valueOf (node.uptime_number));
        }
        else if (token3.equalsIgnoreCase ("storage")) {
            token3_type = TYPE_NODE_VALUE_FLOAT;
            token3_float = Double.valueOf (String.valueOf (node.current_storage));
        }
        else if (token3.equalsIgnoreCase ("x_coord")) {
            token3_type = TYPE_NODE_VALUE_FLOAT;
            token3_float = ((node.coordinates == null) ? 0.0 : node.coordinates [0]);
        }
        else if (token3.equalsIgnoreCase ("y_coord")) {
            token3_type = TYPE_NODE_VALUE_FLOAT;
            token3_float = ((node.coordinates == null) ? 0.0 : node.coordinates [1]);
        }
        else if (token3.equalsIgnoreCase ("estimate")) {
            token3_type = TYPE_NODE_VALUE_FLOAT;
            token3_float = Double.valueOf (String.valueOf (node.estimate));
        }
        else {
            token3_type = TYPE_LITERAL;
        }


        if (!(token1_type == TYPE_NODE_VALUE_FLOAT || token1_type == TYPE_NODE_VALUE_STRING ||
              token3_type == TYPE_NODE_VALUE_FLOAT || token3_type == TYPE_NODE_VALUE_STRING)) {
            return INCORRECT_INPUT;
        }
        

        if (token2.equalsIgnoreCase (">")) {
            if ((token1_type != TYPE_LITERAL && token1_type != TYPE_NODE_VALUE_FLOAT) ||
                (token3_type != TYPE_LITERAL && token3_type != TYPE_NODE_VALUE_FLOAT)) {
                return INCORRECT_INPUT;
            }

            try {
                if (token1_type == TYPE_LITERAL) {
                    token1_float = Double.parseDouble (token1);
                }
                if (token3_type == TYPE_LITERAL) {
                    token3_float = Double.parseDouble (token3);
                }
            } catch (NumberFormatException e) {
                return INCORRECT_INPUT;
            }

            if (token1_float > token3_float) {
                this_result = MATCHING_NODE;
            }
            else {
                this_result = NOT_MATCHING_NODE;;
            }
        }
        else if (token2.equalsIgnoreCase (">=")) {
            if ((token1_type != TYPE_LITERAL && token1_type != TYPE_NODE_VALUE_FLOAT) ||
                (token3_type != TYPE_LITERAL && token3_type != TYPE_NODE_VALUE_FLOAT)) {
                return INCORRECT_INPUT;
            }

            try {
                if (token1_type == TYPE_LITERAL) {
                    token1_float = Double.parseDouble (token1);
                }
                if (token3_type == TYPE_LITERAL) {
                    token3_float = Double.parseDouble (token3);
                }
            } catch (NumberFormatException e) {
                return INCORRECT_INPUT;
            }

            if (token1_float >= token3_float) {
                this_result = MATCHING_NODE;
            }
            else {
                this_result = NOT_MATCHING_NODE;;
            }
        }
        else if (token2.equalsIgnoreCase ("<")) {
            if ((token1_type != TYPE_LITERAL && token1_type != TYPE_NODE_VALUE_FLOAT) ||
                (token3_type != TYPE_LITERAL && token3_type != TYPE_NODE_VALUE_FLOAT)) {
                return INCORRECT_INPUT;
            }

            try {
                if (token1_type == TYPE_LITERAL) {
                    token1_float = Double.parseDouble (token1);
                }
                if (token3_type == TYPE_LITERAL) {
                    token3_float = Double.parseDouble (token3);
                }
            } catch (NumberFormatException e) {
                return INCORRECT_INPUT;
            }

            if (token1_float < token3_float) {
                this_result = MATCHING_NODE;
            }
            else {
                this_result = NOT_MATCHING_NODE;;
            }
        }
        else if (token2.equalsIgnoreCase ("<=")) {
            if ((token1_type != TYPE_LITERAL && token1_type != TYPE_NODE_VALUE_FLOAT) ||
                (token3_type != TYPE_LITERAL && token3_type != TYPE_NODE_VALUE_FLOAT)) {
                return INCORRECT_INPUT;
            }

            try {
                if (token1_type == TYPE_LITERAL) {
                    token1_float = Double.parseDouble (token1);
                }
                if (token3_type == TYPE_LITERAL) {
                    token3_float = Double.parseDouble (token3);
                }
            } catch (NumberFormatException e) {
                return INCORRECT_INPUT;
            }

            if (token1_float <= token3_float) {
                this_result = MATCHING_NODE;
            }
            else {
                this_result = NOT_MATCHING_NODE;;
            }
        }
        else if (token2.equalsIgnoreCase ("=") || token2.equalsIgnoreCase ("!=")) {
            if (token1_type == TYPE_NODE_VALUE_STRING) {
                if (token3_type == TYPE_NODE_VALUE_STRING ||
                    token3_type == TYPE_LITERAL) {
                    if (check_some_substring_matches (token1, token3)) {
                        this_result = MATCHING_NODE;
                    }
                    else {
                        this_result = NOT_MATCHING_NODE;
                    }
                }
                else {
                    return INCORRECT_INPUT;
                }
            }
            else if (token1_type == TYPE_NODE_VALUE_FLOAT) {
                if (token3_type == TYPE_NODE_VALUE_FLOAT) {
                    if (token1_float == token3_float) {
                        this_result = MATCHING_NODE;
                    }
                    else {
                        this_result = NOT_MATCHING_NODE;
                    }
                }
                else if (token3_type == TYPE_LITERAL) {
                    try {
                        token3_float = Double.parseDouble (token3);
                    } catch (NumberFormatException e) {
                        return INCORRECT_INPUT;
                    }
                    if (token1_float == token3_float) {
                        this_result = MATCHING_NODE;
                    }
                    else {
                        this_result = NOT_MATCHING_NODE;
                    }
                }
                else {
                    return INCORRECT_INPUT;
                }
            }
            else if (token1_type == TYPE_LITERAL) {
                if (token3_type == TYPE_NODE_VALUE_FLOAT) {
                    try {
                        token1_float = Double.parseDouble (token1);
                    } catch (NumberFormatException e) {
                        return INCORRECT_INPUT;
                    }
                    if (token1_float == token3_float) {
                        this_result = MATCHING_NODE;
                    }
                    else {
                        this_result = NOT_MATCHING_NODE;
                    }
                }
                else if (token3_type == TYPE_NODE_VALUE_STRING) {
                    if (check_some_substring_matches (token1, token3)) {
                        this_result = MATCHING_NODE;
                    }
                    else {
                        this_result = NOT_MATCHING_NODE;
                    }
                }
                else {
                    return INCORRECT_INPUT;
                }
            }
            else {
                return INCORRECT_INPUT;
            }
            
            if (token2.equalsIgnoreCase ("!=")) {
                if (this_result == MATCHING_NODE) {
                    this_result = NOT_MATCHING_NODE;
                }
                else {
                    this_result = MATCHING_NODE;
                }
            }
        }
        else if (token2.equalsIgnoreCase ("&")) {
            return INCORRECT_INPUT;
        }
        else {
            return INCORRECT_INPUT;
        }

            
        if (this_result == NOT_MATCHING_NODE) {
            return NOT_MATCHING_NODE;
        }
        else if (!check_bounds (index, tokens)) {
            return this_result;
        }
        else {
            token4 = String.valueOf ((char []) tokens.get (index));
            if (token4.equalsIgnoreCase ("&")) {
                return interpret_tokens (index+1, tokens, node);
                
            }
            else {
                return INCORRECT_INPUT;
            }
        }
    }

    protected boolean check_some_substring_matches (String a, String b) {
        String short_string;
        String long_string;

        if (a.length () < b.length ()) {
            short_string = a;
            long_string = b;
        }
        else {
            short_string = b;
            long_string = a;
        }

        for (int x = 0; x < long_string.length (); x++) {
            if (long_string.regionMatches (true, x, short_string, 0, short_string.length ())) {
                return true;
            }
        }
        return false;
    }

    protected boolean check_bounds (int index, LinkedList list) {
        if (index < list.size ()) {
            return true;
        }
        else {
            return false;
        }
    }

    /*
      Dumps all nodes information to a file specified by the given file_name.  The format is:
      build hostname ip port uptime (in seconds) storage (in Mbs) x_coord y_coord estimate (of network size)
      Each field is separated by a single space and each node entry is on a new line.
    */
    public boolean dump_node_information_to_file (String file_name) {
        File F;
        FileOutputStream FOS;
        PrintStream P;
        try {
            F = new File (file_name);
            F.delete ();
            F.createNewFile ();
            
            FOS = new FileOutputStream (F);
            P = new PrintStream (FOS);
        } catch (Exception e) {
            return false;
        }

        Iterator i = get_all_nodes ();
        while (i.hasNext ()) {
            BambooNode node = (BambooNode) i.next ();
            String s = (node.build + " " + node.hostname + " " + node.IP + " " + node.port + " " + node.uptime_number + " " + 
                        node.current_storage + " " + ((node.coordinates == null) ? 0.0 : node.coordinates [0]) + " " +
                        ((node.coordinates == null) ? 0.0 : node.coordinates [1]) + " " + node.estimate) + '\n';
            
            try {
                P.print (s);
            } catch (Exception e) {
                return false;
            }
        }

        try {
            P.close ();
        } catch (Exception e) {
            return false;
        }

        return true;
    }
    
    /*
      There are two things you can do by invoking main:
      If you give no command line arguments, a simple test will be run.
      See the simple test to see an example of how to easily start FetchNodeInfoThread
      and retrieve information about all the nodes.
      You can give the command line argument -d and then a number and a string to
      have FetchNodeInfoThread dump out all node information to a file specified by
      the string every certain millisecond time, specified by a number.  For example:
      ../../../bin/run-java -mx32M bamboo.vis.FetchNodeInfoThread -d 60000 temp.info
      This will cause the information about all nodes to be dumped to the file temp.info
      every minute, each dump completely replaces the information in the file.
      The format of this file is specified by the function dump_node_information_to_file
    */
    public static void main (String [] args) throws IOException {
        if (args.length > 0 && args[0].equals("-d")) {
            int refresh_period;
            String file_name;
            
            if (args.length != 3) {
                System.out.println ("Bad number of arguments given");
                return;
            }

            try {
                refresh_period = Integer.valueOf (args[1]);
            } catch (NumberFormatException e) {
                System.out.println ("Bad number for refersh period given");
                return;                
            }
            file_name = args[2];

            FetchNodeInfoThread t = new FetchNodeInfoThread (null, null);
            
            t.run ();

            try {
                Thread.sleep (500);
            } catch (Exception e) {
                System.out.print("Problem getting FetchNodeInfoThread to sleep, terminating...");
                t = null;
                return;
            }

            if (!t.dump_node_information_to_file (file_name)) {
                System.out.println ("Couldn't dump information to file.");
            } 

            while (true) {
                try {
                    Thread.sleep (refresh_period);
                } catch (Exception e) {
                    System.out.print("Problem getting FetchNodeInfoThread to sleep, terminating...");
                    t = null;
                    return;
                }

                if (!t.dump_node_information_to_file (file_name)) {
                    System.out.println ("Couldn't dump information to file.");
                }
            }
        }
        else {
            /*
              This is an example of how to get FetchNodeInfoThread to start running.
              The two arguments you give it are two Thunks, one which specifies what to do when
              a node is successfully retrieved and the other what to do when a node fails to be retrieved.
              You can make either of these arguments null and there will be no special actions taken.
              The Thunks are passed the BambooNode that failed or is successfully retrieved, be aware
              that in the event of a failure the argument may be null.
              After run () is invoked, you can use the HashMap nodes_by_id<id, BambooNode> to retrieve specific nodes
              or the helper function get_all_nodes to get an Iterator over all nodes retrieved so far.
              However, you should allow some time (a few seconds) for FetchNodeInfoThread to retrieve information
              on all the nodes.  If you try to query the nodes too early, FetchNodeInfoThread
              may not have had time to receive responses from all the nodes and so the set of nodes
              it has may be incomplete.  Another thing, you should run FetchNodeInfoThread only with an
              enlarged memory size for java, otherwise it will run out of memory.  To do this use the command:
              ../../../bin/run-java -mx32M bamboo.vis.FetchNodeInfoThread
              This should be run from the bamboo vis directory.
            */
            Thunk1<BambooNode> fetch_succeeded = new Thunk1<BambooNode> () {
                int count = 0;
                
                public void run (BambooNode node) {
                    count++;
                    System.out.println (count + ":\t" + node.IP + ":" + node.port + "\t" + node.hostname);
                }
            };
            
            FetchNodeInfoThread t = new FetchNodeInfoThread (fetch_succeeded, null);
            
            t.run ();
        }
    }
}
