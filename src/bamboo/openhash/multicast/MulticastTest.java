/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.openhash.multicast;
import bamboo.dht.*;
import bamboo.lss.ASyncCore;
import bamboo.lss.DustDevil;
import bamboo.util.GuidTools;
import bamboo.util.StandardStage;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import org.apache.log4j.Level;
import ostore.util.ByteUtils; // for print_bytes function only
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QuickSerializable;
import ostore.util.QSException;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.EventHandlerIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import seda.sandStorm.api.StageIF;
import static bamboo.util.Curry.*;

/**
 * Test class for implementation of multicast
 * Joins have TTL 60 seconds
 * Clients re-join every 30 seconds, on average
 */
public class MulticastTest  extends StandardStage
implements SingleThreadedEventHandlerIF {

    protected static final String APP = 
        "bamboo.openhash.multicast.MulticastTest $Revision: 1.25 $";
    protected MulticastClient client;
    protected Random rand;
    protected BigInteger groupname;
    protected bamboo.lss.ASyncCore acore;
    protected MessageDigest digest;
    protected int messageNumber;
    protected int nodes;
    protected int interval_sec;

    protected final int JOIN_TTL = 60;

    public void init (ConfigDataIF config) throws Exception {
	super.init (config);
        int debug_level = config_get_int (config, "debug_level");
        if (debug_level > 0) 
	    logger.setLevel (Level.DEBUG);
        acore = DustDevil.acore_instance ();
        int seed = config_get_int (config, "seed");
        if (seed == -1)
            seed = ((int) now_ms ()) ^ my_node_id.hashCode();
        rand = new Random (seed);
        String groupname_str = config_get_string (config, "groupname");
        MessageDigest digest = null;
        try { digest = MessageDigest.getInstance("SHA"); }
        catch (Exception e) { assert false; }
        groupname = MulticastClient.bytes2bi (
                digest.digest (groupname_str.getBytes ()));
        String client_stg_name = 
            config_get_string (config, "client_stage_name");
        StageIF client_stg = config.getManager ().getStage (client_stg_name);
        client = (MulticastClient) client_stg.getWrapper ().getEventHandler ();

	messageNumber = 0;
	nodes = config_get_int (config, "nodes");
	interval_sec = config_get_int (config, "interval_sec");
	acore.register_timer (0, new ReadyCb (), null);
    }

    public class ReadyCb implements ASyncCore.TimerCB {
        public void timer_cb (Object user_data) {
            try { client.registerReceiver(Payload.class, recv); }
            catch (Exception e) { assert false : e; }
            acore.register_timer (0, new JoinCb (), null);
	    acore.register_timer (5000, new SendCb(), null);
	}
    }

    protected Thunk1<MulticastMessage> recv = new Thunk1<MulticastMessage>() {
        public void run(MulticastMessage msg) {
            Payload p = (Payload) msg.payload;
            logger.info("received msg " + p.seq + " from " + p.source);
        }
    };
     
    /* Rejoin group and rebuild tree every 35-55 seconds*/
    public class JoinCb implements ASyncCore.TimerCB { 
        public void timer_cb (Object user_data) {
	    client.join (groupname, JOIN_TTL, APP, new JoinDoneCb (), null);
	    int random = rand.nextInt(21);
            acore.register_timer ((30 + random) * 1000, this, null);
        }
    }
    
    public class JoinDoneCb implements ASyncCore.TimerCB {
	public void timer_cb (Object user_data) {
	    
	    /* Output of "up to level" a little bit deceptive when node
	       hashes to the same partition as the actual groupname 
	    */
	    logger.info("Joined groupname=0x" 
			+ GuidTools.guid_to_string(groupname)
			+ " up to level " + 
			+ ((Integer)user_data).intValue() 
			+ " with ttl_s=" + JOIN_TTL);

	}
    }

    public class SendCb implements ASyncCore.TimerCB {
        public void timer_cb (Object user_data) {
	    client.sendMsg(groupname, new Payload(my_node_id, messageNumber));
	    logger.info ("Message number " + messageNumber + " done sending.");
	    messageNumber++;
	    acore.register_timer (5 * 1000, new SendCb (), null);
        }
    };

    public static class Payload implements QuickSerializable {
        public NodeId source;
        public int seq;
        public Payload(NodeId source, int seq) { 
            this.source = source;
            this.seq = seq;
        }
        public Payload(InputBuffer buffer) throws QSException { 
            source = new NodeId(buffer);
            seq = buffer.nextInt(); 
        }
        public void serialize(OutputBuffer buffer) { 
            source.serialize(buffer);
            buffer.add(seq); 
        }
        public Object clone(Object other) throws CloneNotSupportedException { 
            Payload result = (Payload) super.clone(); 
            result.source = source;
            result.seq = seq;
            return result;
        }
        public String toString() { 
            return "(Payload source=" + source + " seq=" + seq + ")";
        }
    }
}

