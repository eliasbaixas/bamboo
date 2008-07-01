/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.router;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import ostore.network.NetworkMessageResult;
import ostore.util.NodeId;
import bamboo.util.StandardStage;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import ostore.network.NetworkMessageResult;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import bamboo.lss.ASyncCore;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;

/**
 * A simple regression test for the DataManager.
 *
 * @author Sean C. Rhea
 * @version $Id: CheckRunning.java,v 1.14 2004/08/04 17:45:26 srhea Exp $
 */
public class CheckRunning extends StandardStage
implements SingleThreadedEventHandlerIF {

    protected LinkedList hosts = new LinkedList ();
    protected Set outstanding = new HashSet ();
    protected PrintWriter log;

    public CheckRunning () {
        event_types = new Class [] { NetworkMessageResult.class };
    }

    public void init (ConfigDataIF config) throws Exception {
        super.init (config);
        int port = config_get_int (config, "port");
        String host_file = config_get_string (config, "host_file");
        BufferedReader in = new BufferedReader (new FileReader (host_file));
        String line = null;
        while ((line = in.readLine ()) != null) {
            InetAddress addr = null;
	    try {
		addr = InetAddress.getByName(line);
	    }
	    catch (UnknownHostException e) {
                logger.fatal ("could not create address from " + line);
                assert false;
	    }
	    NodeId node_id = new NodeId (5850, addr);
	    hosts.addLast (node_id);
        }
        String log_file = config_get_string (config, "log_file");
        log = new PrintWriter (new BufferedWriter (
                    new FileWriter (log_file, true /* append */))); 
        int concurrent = config_get_int (config, "concurrent");
        for (int i = 0; i < concurrent; ++i)
            acore.register_timer (0, ping_cb, null);
    }

    public void handleEvent (QueueElementIF item) {
        if (item instanceof NetworkMessageResult) {
            NetworkMessageResult result = (NetworkMessageResult) item;
            NodeId node = (NodeId) result.user_data;
            outstanding.remove (node);
            if (result.success)
                log.println (now_ms () + " " + node + " 1");
            else
                log.println (now_ms () + " " + node + " 0");

            ping_cb.timer_cb (null);
        }
        else {
            BUG ("unexpected event: " + item);
        }
    }

    public ASyncCore.TimerCB ping_cb = new ASyncCore.TimerCB () {
        public void timer_cb (Object user_data) {
            if (hosts.isEmpty ()) {
                if (outstanding.isEmpty ()) {
                    logger.info ("all done");
                    System.exit (0);
                }
                return;
            }

            NodeId host = (NodeId) hosts.removeFirst ();
            outstanding.add (host);
            PingMsg ping = new PingMsg (host);
            ping.comp_q = my_sink;
            ping.user_data = host;
            ping.timeout_sec = 60;
            dispatch (ping);
        }
    };
}

