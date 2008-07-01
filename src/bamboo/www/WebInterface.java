/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.www;

import bamboo.api.BambooNeighborInfo;
import bamboo.db.StorageManager;
import bamboo.lss.ASyncCore;
import bamboo.router.NeighborInfo;
import bamboo.router.Router;
import bamboo.router.RoutingTable;
import bamboo.util.GuidTools;
import bamboo.util.MultipleByteBufferInputStream;
import bamboo.util.StringUtil;
import bamboo.vivaldi.VirtualCoordinate;
import bamboo.vivaldi.VivaldiReplyVC;
import bamboo.vivaldi.VivaldiRequestVC;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.text.NumberFormat;
import java.util.*;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.xmlrpc.*;
import ostore.network.NetworkLatencyReq;
import ostore.network.NetworkLatencyResp;
import ostore.util.NodeId;
import ostore.util.SecureHash;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;

import static bamboo.util.Curry.*;

/**
 * A web interface to the router's state.
 *
 * @author Sean C. Rhea
 * @version $Id: WebInterface.java,v 1.32 2005/12/14 19:23:46 srhea Exp $
 */
public class WebInterface extends bamboo.util.StandardStage
implements StorageManager.StorageMonitor {

    protected static HashMap<NodeId,WebInterface> instances = 
        new HashMap<NodeId,WebInterface> ();

    public static WebInterface instance (NodeId node_id) {
        return instances.get (node_id);
    }

    public static class MyReqProc extends XmlRpcRequestProcessor {
        public MyReqProc () { super (); }
    }

    public static final int CR = 0x0D;
    public static final int LF = 0x0A;

    protected int DIGIT_VALUES;
    protected BigInteger MODULUS;
    protected BigInteger my_guid;
    protected BambooNeighborInfo my_neighbor_info;

    protected long init_start_time_ms;

    protected BambooNeighborInfo [] preds;
    protected BambooNeighborInfo [] succs;

    protected Random rand;
    protected int search_leaf_set_ttl;

    protected RoutingTable rt;
    protected Map latencies = new HashMap ();

    protected int listen_port;
    protected ServerSocketChannel svr_ch;
    protected WebAppender web_appender;
    protected String hostname;
    protected String build = "unknown";

    protected HashMap<String,Object> handlers = new HashMap<String,Object> ();

    public void register_xml_rpc_handler (String name, Object handler) {
        handlers.put (name, handler);
    }

    protected long total_storage = 0;
    public void storage_changed(boolean added,
                                InetAddress client_id, long size) {
        if (added)
            total_storage += size;
        else
            total_storage -= size;
    }

    protected VirtualCoordinate coordinate;

    public class CoordinateAlarmCb implements ASyncCore.TimerCB {
        public void timer_cb (Object user_data) {
            dispatch (new VivaldiRequestVC (my_sink, null));
        }
    }

    public static class PingHandler {
        public void ping (String user_agent, InetSocketAddress client, 
                          Thunk1<Object> result_cb) {
            // XML RPC dictates that we have to return something.
            result_cb.run(client.getAddress ().getHostAddress () + ":" +
                          client.getPort () + ", " + user_agent);
        }
    }

    public WebInterface() throws Exception {
	DEBUG = false;
        handlers.put ("ping", new PingHandler ());
    }

    public void init (ConfigDataIF config) throws Exception {
	super.init (config);

        String build_file = config_get_string(config, "build_file");
        if (build_file != null) {
            try {
                InputStream is = new FileInputStream(build_file);
                Reader reader = new BufferedReader(new InputStreamReader(is));
                StreamTokenizer tok = new StreamTokenizer(reader);
                tok.eolIsSignificant(false);
                tok.parseNumbers();
                tok.nextToken();
                if (tok.ttype == StreamTokenizer.TT_NUMBER)
                    build = "" + (int) tok.nval;
                else
                    logger.info("not a word");
            }
            catch (Exception e) {
                logger.warn("couldn't open build file");
            }
        }

        instances.put (my_node_id, this);
        if (config_get_boolean (config, "include_logs")) {
            PatternLayout pl =
                new PatternLayout ("%d{ISO8601} %-5p %c: %m\n");
            web_appender = new WebAppender (25);
            web_appender.setLayout (pl);
            Logger.getRoot ().addAppender (web_appender);
        }
	init_start_time_ms = now_ms ();
        listen_port = my_node_id.port () + 1;
        latencies.put (my_node_id, new Long (0));
        classifier.dispatch_later (new VivaldiRequestVC (my_sink, null), 5000);
        String sm_name = config_get_string (config, "storage_manager_stage");
        StorageManager sm = (StorageManager) lookup_stage (config, sm_name);
        sm.register_monitor (this);
        hostname = my_node_id.address ().getHostName ();
        acore.registerTimer(0, ready);
    }

    public void handleEvent (QueueElementIF item) {
	if (logger.isDebugEnabled ()) logger.debug ("got " + item);

        if (item instanceof VivaldiReplyVC) {
            VivaldiReplyVC reply = (VivaldiReplyVC) item;
            coordinate = reply.coordinate;
            classifier.dispatch_later (
                    new VivaldiRequestVC (my_sink, null), 5000);
        }
	else if (item instanceof NetworkLatencyResp) {
            NetworkLatencyResp resp = (NetworkLatencyResp) item;
            if (resp.success)
                latencies.put (resp.user_data, new Long (resp.rtt_ms));
        }
        else {
            BUG("got event of unexpected type: " + item + ".");
        }
    }

    protected Runnable ready = new Runnable() {
        public void run() {

            Router router = Router.instance(my_node_id);
            try {
                router.registerApplication(
                        Router.applicationID(WebInterface.class),
                        lsChanged, rtChanged, null, null, null);
            }
            catch (Router.DuplicateApplicationException e) { BUG(e); }

            MODULUS = router.modulus();
            DIGIT_VALUES = router.valuesPerDigit();

            my_guid = router.id();
            my_neighbor_info =
                new BambooNeighborInfo (my_node_id, my_guid, 0.0);

            rt = new RoutingTable (new NeighborInfo (my_node_id, my_guid),
                    1.0, MODULUS, router.digitsPerID(), DIGIT_VALUES);

            {
                SecureHash my_guid_sh =
                    GuidTools.big_integer_to_secure_hash (my_guid);
                byte [] noise = my_guid_sh.bytes ();
                int seed = ostore.util.ByteUtils.bytesToInt (
                        noise, new int [1]);
                rand = new Random (seed);
            }

            try {
                svr_ch = ServerSocketChannel.open ();
                svr_ch.configureBlocking (false);
                SocketAddress addr =
                    new InetSocketAddress (my_node_id.address (), listen_port);
                ServerSocket sock = svr_ch.socket ();
                sock.bind (addr);
                acore.register_selectable (svr_ch, SelectionKey.OP_ACCEPT, 
                        new ASyncCore.SelectableCB () {
                            public void select_cb (SelectionKey skey,
                                                   Object user_data) {
                                if (skey.isAcceptable ())
                                    handle_accept_ready ();
                            }
                        }, null);
            }
            catch (IOException e) {
                BUG (e);
            }

            acore.register_timer (10*750 + rand.nextInt (10*500),
                    periodic_timer_cb, null);
        }
    };

    protected static class ConnState {
        public SocketChannel channel;
        public int req_idx = 0;
        public ByteBuffer req = ByteBuffer.wrap (new byte [4096]);
        public ByteBuffer resp;
        public LinkedList<String> header = new LinkedList<String> ();
        public SelectionKey skey;
        public int outstandingReqs;
        public ConnState (SocketChannel c) {
            channel = c;
        }
    }

    protected ASyncCore.TimerCB periodic_timer_cb = new ASyncCore.TimerCB () {
        public void timer_cb (Object user_data) {

            if (preds != null) {
                for (int i = preds.length - 1; i >= 0; --i)
                    dispatch (new NetworkLatencyReq (preds [i].node_id,
                                my_sink, preds [i].node_id));
            }

            if (succs != null) {
                for (int i = succs.length - 1; i >= 0; --i)
                    dispatch (new NetworkLatencyReq (succs [i].node_id,
                                my_sink, succs [i].node_id));
            }

            for (int i = 0; i <= rt.highest_level (); ++i) {
                for (int j = 0; j < DIGIT_VALUES; ++j) {
                    RoutingTable.RoutingEntry re = rt.primary_re (i, j);
                    if (re != null) {
                        BambooNeighborInfo ni = new BambooNeighborInfo (
                                re.ni.node_id, re.ni.guid, re.rtt_ms);
                        dispatch (new NetworkLatencyReq (re.ni.node_id,
                                    my_sink, re.ni.node_id));
                    }
                }
            }

            acore.register_timer (10*750 + rand.nextInt (10*500),
                    periodic_timer_cb, null);
        }
    };

    protected void handle_accept_ready () {
        SocketChannel channel = null;
        try {
            channel = svr_ch.accept ();
            if (channel == null)
                return;
            channel.configureBlocking (false);
            ConnState state = new ConnState (channel);

            InetSocketAddress remote = (InetSocketAddress)
                channel.socket ().getRemoteSocketAddress ();
            logger.info ("got connection from " +
                    remote.getAddress ().getHostAddress () + ":" +
                    remote.getPort ());
            
            state.skey = acore.register_selectable (channel,
                    SelectionKey.OP_READ, new ASyncCore.SelectableCB () {
                        public void select_cb (SelectionKey skey, Object ud) {
                            if (((skey.interestOps () &
                                  SelectionKey.OP_READ) != 0)
                                && skey.isReadable ())
                                handle_read_ready (skey, (ConnState) ud);
                            // handle_read_ready may have closed the socket,
                            // so check for valid first
                            if (skey.isValid () &&
                                ((skey.interestOps () &
                                  SelectionKey.OP_WRITE) != 0)
                                && skey.isWritable ())
                                handle_write_ready (skey, (ConnState) ud);
                        }
                    }, state);
            handle_read_ready(state.skey, state);

            acore.register_timer (60*1000, conn_timeout_cb, state);
        }
        catch (IOException e) {
            BUG (e);
        }
    }

    ASyncCore.TimerCB conn_timeout_cb = new ASyncCore.TimerCB () {
        public void timer_cb (Object user_data) {
            ConnState state = (ConnState) user_data;
            if (state.outstandingReqs > 0) {
                acore.register_timer (60*1000, conn_timeout_cb, state);
            }
            else if (state.skey.isValid ()) {
                InetSocketAddress remote = (InetSocketAddress)
                    state.channel.socket ().getRemoteSocketAddress ();
                logger.info ("timeout on connection to " + 
                        remote.getAddress ().getHostAddress () + ":" +
                        remote.getPort ());
                close (state);
            }
        }
    };

    protected void handle_read_ready (final SelectionKey skey, 
                                      final ConnState state) {

        // Read any new data.

        while (true) {
            if (state.req.position () == state.req.limit ()) {
                // too big
                close (state);
                return;
            }
            int count = 0;
            try {
                count = state.channel.read (state.req);
            }
            catch (IOException e) {
                close (state);
                return;
            }
            if (count < 0) {
                close (state);
                return;
            }
            if (count == 0) {
                break;
            }
        }

        if (state.header.isEmpty () || 
            (! state.header.getLast ().equals (""))) {

            // Read any new header lines.

            boolean have_cr = false;
            for (int j = state.req_idx; j <= state.req.position (); ++j) {
                if (state.req.get (j) == CR) {
                    have_cr = true;
                }
                else if (have_cr) {
                    if (state.req.get (j) == LF) {
                        String line = new String (
                                state.req.array (), 
                                state.req.arrayOffset () + state.req_idx, 
                                j - state.req_idx - 1);
                        if (logger.isDebugEnabled ())
                            logger.debug ("read header line: \"" + line + "\"");
                        state.req_idx = j + 1;
                        if (line.equals ("")) {
                            state.header.addLast (line);
                            break;
                        }
                        else {
                            state.header.addLast (line);
                        }
                    }
                    else {
                        have_cr = false;
                    }
                }
            }
        }

        if ((! state.header.isEmpty ()) &&
            (state.header.getLast ().equals (""))) {

            // We're done reading the header.

            StringTokenizer tokens = 
                new StringTokenizer (state.header.getFirst ());
            String method = tokens.nextToken();

            if (method.equals ("GET")) {
                skey.interestOps (SelectionKey.OP_WRITE);
                create_response (state);
                return;
            }
            else if (method.equals ("POST")) {
                int contentLength = -1;
                String userAgent = "XML: unknown";
                for (String line : state.header) {
                    String lineLower = line.toLowerCase();
                    if (lineLower.startsWith("content-length:")) {
                        contentLength = Integer.parseInt(
                                line.substring(15).trim());
                    }
                    if (lineLower.startsWith("user-agent:")) {
                        userAgent = "XML: " + line.substring(11).trim();
                    }
                }

                if (contentLength == -1) {
                    close (state);
                    return;
                }

                if ((state.req.position () - state.req_idx) < contentLength) {
                    // Still need more bytes of the body.
                    return;
                }

                // Stop reading bytes.

                skey.interestOps (0);

                MultipleByteBufferInputStream is = 
                    new MultipleByteBufferInputStream ();
                state.req.limit (state.req.position ());
                state.req.position (state.req_idx);
                is.add_bb (state.req);

                final MyReqProc requestProcessor = new MyReqProc ();
                final XmlRpcResponseProcessor responseProcessor = 
                    new XmlRpcResponseProcessor ();
                XmlRpcRequest request = null;

                try {
                    request = requestProcessor.processRequest(is);
                }
                catch (Exception e) {
                    close (state);
                    return;
                }

                Class[] argClasses = null;
                Object[] argValues = null;

                Vector params = request.getParameters ();
                if (params == null) {
                    argClasses = new Class[3];
                    argValues = new Object[3];
                }
                else {
                    argClasses = new Class[params.size() + 3];
                    argValues = new Object[params.size() + 3];
                    for (int i = 0; i < params.size(); i++) {
                        argValues[i] = params.elementAt(i);
                        if (argValues[i] instanceof Integer)
                            argClasses[i] = Integer.TYPE;
                        else if (argValues[i] instanceof Double)
                            argClasses[i] = Double.TYPE;
                        else if (argValues[i] instanceof Boolean)
                            argClasses[i] = Boolean.TYPE;
                        else
                            argClasses[i] = argValues[i].getClass();
                    }
                }

                Thunk1<Object> result_cb = new Thunk1<Object> () {
                    public void run(Object response) {
                        state.outstandingReqs--;
                        byte [] resp = null;
                        if (response instanceof Exception) {
                            resp = responseProcessor.processException (
                                    (Exception) response, 
                                    requestProcessor.getEncoding());
                        }
                        else {
                            try {
                                resp = responseProcessor.processResponse (
                                        response, 
                                        requestProcessor.getEncoding());
                            }
                            catch (Exception e) {
                                resp = responseProcessor.processException (
                                        e, requestProcessor.getEncoding());
                            }
                        }
                        // Make sure the client's still waiting on the
                        // response (it hasn't timed out and been closed).
                        if (skey.isValid ()) {
                            skey.interestOps (SelectionKey.OP_WRITE);
                            create_response (state, resp);
                        }
                    }
                };

                InetSocketAddress remote = (InetSocketAddress)
                    state.channel.socket ().getRemoteSocketAddress ();

                argClasses [argClasses.length - 3] = String.class;
                argValues [argValues.length - 3] = userAgent;
                argClasses [argClasses.length - 2] = InetSocketAddress.class;
                argValues [argValues.length - 2] = remote;
                argClasses [argClasses.length - 1] = Thunk1.class;
                argValues [argValues.length - 1] = result_cb;

                Object invokeTarget = handlers.get (request.getMethodName ());

                String methodName = request.getMethodName();

                Method fn = null;

                // The last element of the XML-RPC method name is the Java
                // method name.
                int dot = methodName.lastIndexOf('.');
                if (dot > -1 && dot + 1 < methodName.length())
                    methodName = methodName.substring(dot + 1);

                try {
                    if (invokeTarget == null) {
                        throw new Exception ("no such handler: " +
                                request.getMethodName ());
                    }
                    Class targetClass = invokeTarget.getClass ();
                    fn = targetClass.getMethod(methodName, argClasses);
                    if (fn.getDeclaringClass() == Object.class) {
                        throw new Exception("Invoker can't call methods "
                                + "defined in java.lang.Object");
                    }
                    // The return value is passes as an argument to result_cb.
                    state.outstandingReqs++;
                    fn.invoke(invokeTarget, argValues);
                }
                catch (Exception e) {
                    logger.error("request caused exception", e);
                    byte [] resp = responseProcessor.processException (
                            e, requestProcessor.getEncoding());
                    skey.interestOps (SelectionKey.OP_WRITE);
                    create_response (state, resp);
                    return;
                }
            }
        }
    }

    protected void close (ConnState state) {
        acore.unregister_selectable (state.skey);
        try {
            state.channel.socket ().close ();
        }
        catch (IOException e) {
            BUG (e); // TODO
        }
    }

    protected void handle_write_ready (SelectionKey skey, ConnState state) {
        int count = 1;
        while ((count > 0) && (state.resp.position () != state.resp.limit ())) {
            try {
                count = state.channel.write (state.resp);
            }
            catch (IOException e) {
                close (state);
                return;
            }
        }
        if (state.resp.position () == state.resp.limit ()) {
            close (state);
        }
    }

    protected void print_ls_table_row (StringBuffer r, int pos, 
                                       BambooNeighborInfo ni, Long rtt_ms) {

        r.append ("<tr><td align=\"center\">");
        r.append (pos);
        r.append ("</td><td align=\"center\"><a href=\"http://");
        r.append (ni.node_id.address ().getHostAddress ());
        r.append (":");
        r.append (ni.node_id.port () + 1);
        r.append ("/\">");
        r.append (ni.node_id.address ().getHostAddress ());
        r.append ("</a></td><td align=\"center\">");
        r.append (ni.node_id.port ());
        r.append ("</td><td align=\"center\">0x");
        r.append (GuidTools.guid_to_string (ni.guid));
        r.append ("</td><td align=\"center\">");
        if (rtt_ms == null)
            r.append ("??");
        else
            r.append (rtt_ms);
        r.append ("</td></tr>\n");
    }

    protected void create_response (ConnState state, byte [] resp) {
        StringBuffer r = new StringBuffer (1024 + resp.length);
        r.append("HTTP/1.0 200 OK\r\n");
        r.append("Date: Wed, 19 Nov 2003 01:49:00 GMT\r\n");
        r.append("Server: bamboo\r\n");
        r.append("Connection: close\r\n");
        r.append("Content-Type: text/xml\r\n");
        r.append("Content-Length: " + resp.length + "\r\n");
        r.append("\r\n");
        r.append(new String (resp));
        state.resp = ByteBuffer.wrap (r.toString ().getBytes ());
    }

    protected void create_response (ConnState state) {
        StringBuffer r = new StringBuffer (10*1024);

        r.append("HTTP/1.0 200 OK\n");
        r.append("Date: Wed, 19 Nov 2003 01:49:00 GMT\n");
        r.append("Server: bamboo\nConnection: close\n");
        r.append("Content-Type: text/html; charset=iso-8859-1\r\n\r\n");
        r.append("<http>\n<head>\n<title>Bamboo Node ");
        r.append(hostname);
        r.append(':');
        r.append(my_node_id.port ());
        r.append("</title>\n<style type=\"text/css\">\n");
        r.append("body { margin-top:  1em; margin-bottom: 1em;\n");
        r.append("margin-left: 1em; margin-right:  1em; }\n");
        r.append("</style>\n</head>\n<body>\n");
        r.append("<center><h2>Bamboo Node Status Page</h2>\n");
        r.append("<table>\n");
        r.append("<tr><td width=\"40%\"align=\"center\"><h3>Node Info</h3></td>\n");
        r.append("<td width=\"10%\"> </td>\n");
        r.append("<td><h3 width=\"50%\"align=\"center\">Leaf Set</h3></td>\n");
        r.append("</tr>\n<tr><td align=\"center\" valign=\"top\">\n<table>\n");

        r.append("<tr><td><em>Build:</em></td><td width=10></td><td>");
        r.append(build);
        r.append("</td></tr>\n");

        r.append("<tr><td><em>Hostname:</em></td><td></td><td>");
        r.append(hostname);
        r.append("</td></tr>\n");

        r.append("<tr><td><em>IP Address:</em></td><td></td><td>");
        r.append(my_node_id.address ().getHostAddress ());
        r.append("</td></tr>\n");

        r.append("<tr><td><em>Port:</em></td><td></td><td>");
        r.append(my_node_id.port ());
        r.append("</td></tr>\n");

        r.append("<tr><td><em>ID:</em></td><td></td><td>0x");
        r.append(GuidTools.guid_to_string (my_guid));
        r.append("</td></tr>");

        r.append("<tr><td><em>Uptime:</em></td><td></td><td>");
        long uptime_s = (now_ms () - init_start_time_ms) / 1000;
        StringUtil.time_to_sbuf (uptime_s, r);
        r.append("</td></tr>\n");

        r.append("<tr><td><em>Current Storage:</em></td><td></td><td>");
        StringUtil.byte_cnt_to_sbuf (total_storage, r);
        r.append("</td></tr>\n");

        r.append("<tr><td><em>Virtual Coordinates:</em></td><td></td><td>");
        if (coordinate != null) {
            double [] c = coordinate.getCoordinates ();
            NumberFormat n = NumberFormat.getInstance ();
            n.setMaximumFractionDigits(2);
            n.setGroupingUsed (false);
            for (int i = 0; i < c.length; ++i) {
                r.append(n.format(c [i]));
                if (i < c.length - 1)
                    r.append (", ");
            }
        }
        r.append("</td></tr>\n");

        r.append("<tr><td><em>Estimated Network Size:</em></td><td></td><td>");
        BigDecimal nodes = new BigDecimal (1.0);
        if ((preds != null) && (succs != null)
            && (preds.length > 0 && succs.length > 0) 
            && (!preds[preds.length - 1].guid.equals(
                    succs[succs.length - 1].guid))){

            BigDecimal span = new BigDecimal (GuidTools.calc_dist (
                        preds[preds.length-1].guid,
                        succs[succs.length-1].guid,
                        MODULUS));
            BigDecimal slots = new BigDecimal (
                    BigInteger.valueOf (preds.length + succs.length));
            BigDecimal pernode = span.divide (slots, 3, BigDecimal.ROUND_UP);
            nodes = (new BigDecimal (MODULUS)).divide (
                    pernode, 0, BigDecimal.ROUND_UP);
        }
        r.append(nodes);
        r.append(" nodes</td></tr>\n</td>\n<td></td><td>");

        r.append("</table>\n</td><td> </td><td valign=\"top\">");

        r.append("<table width=\"100%\">\n");
        r.append("<tr><td align=\"center\"><em>Position</em></td>");
        r.append("<td align=\"center\"><em>IP Address</em></td>");
        r.append("<td align=\"center\"><em>Port</em></td>");
        r.append("<td align=\"center\"><em>ID</em></td>");
        r.append("<td align=\"center\"><em>RTT (ms)</em></td></tr>\n");

        if (preds != null) {
            for (int i = preds.length - 1; i >= 0; --i) {
                Long rtt_ms = (Long) latencies.get(preds [i].node_id);
                print_ls_table_row(r, -1 * (i + 1), preds [i], rtt_ms);
            }
        }
        if (succs != null) {
            for (int i = 0; i < succs.length; ++i) {
                Long rtt_ms = (Long) latencies.get(succs [i].node_id);
                print_ls_table_row(r, i + 1, succs [i], rtt_ms);
            }
        }

        r.append("</table>\n</td></tr></table>\n");

        r.append("<p><p>\n<table width=\"50%\">\n<tr>\n");
        r.append("<td><h3 align=\"center\">Routing Table</h3></td>\n");
        r.append("</tr>\n<tr>\n<td>\n");

        r.append("<table width=\"100%\">");
        r.append("<tr><td align=\"center\"><em>Level</em></td>");
        r.append("<td align=\"center\"><em>Digit</em></td>");
        r.append("<td align=\"center\"><em>IP Address</em></td>");
        r.append("<td align=\"center\"><em>Port</em></td>");
        r.append("<td align=\"center\"><em>ID</em></td>");
        r.append("<td align=\"center\"><em>RTT (ms)</em></td></tr>");

	for (int i = 0; i <= rt.highest_level (); ++i) {
	    for (int j = 0; j < DIGIT_VALUES; ++j) {
		RoutingTable.RoutingEntry re = rt.primary_re (i, j);
		if ((re != null) && (! re.ni.node_id.equals (my_node_id))) {
                    Long rtt_ms = (Long) latencies.get (re.ni.node_id);

                    r.append ("<tr><td align=\"center\">");
                    r.append (i);
                    r.append ("</td><td align=\"center\">");
                    r.append (j);
                    r.append ("</td><td align=\"center\"><a href=\"http://");
                    r.append (re.ni.node_id.address ().getHostAddress ());
                    r.append (':');
                    r.append (re.ni.node_id.port () + 1);
                    r.append ("/\">");
                    r.append (re.ni.node_id.address ().getHostAddress ());
                    r.append ("</a></td><td align=\"center\">");
                    r.append (re.ni.node_id.port ());
                    r.append ("</td><td align=\"center\">0x");
                    r.append (GuidTools.guid_to_string (re.ni.guid));
                    r.append ("</td><td align=\"center\">");
                    if (rtt_ms == null)
                        r.append ("???");
                    else 
                        r.append (rtt_ms);
                    r.append ("</td></tr>\n");
		}
	    }
        }

        r.append("</table>\n</td>\n</tr>\n</table>\n</center>\n"
                 + "</body>\n</html>\n");

        state.resp = ByteBuffer.wrap (r.toString ().getBytes ());
    }

    protected Thunk2<BambooNeighborInfo[],BambooNeighborInfo[]> lsChanged = 
            new Thunk2<BambooNeighborInfo[],BambooNeighborInfo[]>() {
        public void run(BambooNeighborInfo p[], BambooNeighborInfo s[]) {
            preds = p; succs = s;
        }
    };

    protected Thunk2<BambooNeighborInfo[],BambooNeighborInfo[]> rtChanged = 
            new Thunk2<BambooNeighborInfo[],BambooNeighborInfo[]>() {
        public void run(BambooNeighborInfo add[], BambooNeighborInfo rem[]) {
            if (add != null) {
                for (int i = 0; i < add.length; ++i)
                    rt.force_add(new NeighborInfo(add[i].node_id, add[i].guid),
                                 add[i].rtt_ms);
            }
            if (rem != null) {
                for (int i = 0; i < rem.length; ++i)
                    rt.remove(new NeighborInfo(rem[i].node_id, rem[i].guid));
            }
        }
    };
}

