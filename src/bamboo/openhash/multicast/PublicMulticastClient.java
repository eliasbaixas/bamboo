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
import bamboo.lss.Network;
import bamboo.lss.PriorityQueue;
import bamboo.util.GuidTools;
import bamboo.util.StandardStage;
import java.lang.Integer;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Random;
import org.apache.log4j.Logger;
import ostore.network.NetworkMessage;
import ostore.network.NetworkMessageResult;
import ostore.util.ByteUtils; // for print_bytes function only
import ostore.util.NodeId;
import ostore.util.QuickSerializable;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import seda.sandStorm.api.StageIF;
import static bamboo.openhash.multicast.MulticastClient.*;
import static bamboo.util.Curry.*;

/**
 * An implementation of multicast.
 *
*/
public class PublicMulticastClient {

    protected static final Logger logger =
        Logger.getLogger(PublicMulticastClient.class);

    static {
        try {ostore.util.TypeTable.register_type(MulticastPingMessage.class);}
        catch (Exception e) { assert false : e; }
    }

    protected ASyncCore acore;
    protected GatewayClient client;
    protected MessageDigest digest;
    protected BigInteger groupname;
    protected byte my_addr_bytes[];
    protected BigInteger my_key;
    protected NodeId my_node_id;
    protected Network network;
    protected PriorityQueue queue[];
    protected int replication;
    protected int total_levels;

    protected long now_ms() { 
        return bamboo.util.StandardStage.now_ms(my_node_id);
    }

    protected long timer_ms() { 
        return bamboo.util.StandardStage.timer_ms(my_node_id);
    }

    protected BigInteger partition_number (BigInteger key) {
        BigInteger two2thel = TWO.pow (total_levels - 1);
        BigInteger partition_width = MOD.divide (two2thel);
        // use the key to find the right partition
        BigInteger partition_number = key.divide (partition_width);
        return partition_number;
    }

    protected boolean opposite_partition(
            BigInteger this_key, BigInteger get_key, int level) {
	BigInteger xor = this_key.xor(get_key);
	BigInteger shifted = xor.shiftRight(159 - level);
	return shifted.intValue() == 1;
    }

    protected boolean same_partition(
            BigInteger this_key, BigInteger get_key, int level) {
	BigInteger xor = this_key.xor(get_key);
	BigInteger shifted = xor.shiftRight(160 - level);
	return shifted.intValue() == 0;
    }

    protected int get_recv_level(
            BigInteger this_key, BigInteger recv_key, int num_levels) {
	for (int level = num_levels-1; level >= 0; level--)
	    if (same_partition(this_key, recv_key, level))
		return level;
        assert false; // should always match on level 0...
	return 0;
    }

    public PublicMulticastClient(NodeId my_node_id, long seed, 
            int total_levels, int replication, GatewayClient client, 
            ASyncCore acore) {

        this.acore = acore;
        this.client = client;
        this.my_node_id = my_node_id;
        this.replication = replication;
        this.total_levels = total_levels;

        my_addr_bytes = addr2bytes (my_node_id);
        try { digest = MessageDigest.getInstance("SHA"); }
        catch (Exception e) { assert false; }
        my_key = bytes2bi (digest.digest (my_addr_bytes));

	if (logger.isDebugEnabled ())
	    logger.debug ("my_key=0x" + GuidTools.guid_to_string (my_key));

	queue = new PriorityQueue[total_levels];
	for (int i=0; i < total_levels; i++)
	    queue[i] = new PriorityQueue(10);

        if (logger.isDebugEnabled())
            logger.debug("Key 0x" + GuidTools.guid_to_string(my_key) 
                        + " is in partition " + partition_number(my_key) + ".");

        acore.registerTimer(0, ready);
    }

    protected Runnable ready = new Runnable() {
        public void run() {
            network = Network.instance(my_node_id);
        }
    };

    public static class JoinState {
        public BigInteger groupname;
        public int ttl_s;
        public ASyncCore.TimerCB cb;
        public Object user_data;
        public int current_level;
	public int current_get; //number of nodes at current level
	public boolean done;
	public int done_level;
        public String app;
        public JoinState (BigInteger n, int total_levels, int t, String a, 
                          ASyncCore.TimerCB c, Object u) {
            groupname = n; 
	    ttl_s = t; 
	    cb = c; 
	    user_data = u;
            current_level = total_levels - 1; 
	    current_get = 0;
	    done = false;
	    done_level = -1;
            app = a;
        }
    }

    public void join (BigInteger groupname, int ttl_s, String app, 
                      ASyncCore.TimerCB cb, Object user_data) {

	this.groupname = groupname;

        JoinState state = 
            new JoinState (groupname, total_levels, ttl_s, app, cb, user_data);

	join_get (state);
    }

    protected void join_get (JoinState state) {

        BigInteger key = rendezvous_point (
                my_key, state.groupname, state.current_level);

        bamboo_get_args get_args = new bamboo_get_args ();
        get_args.application = state.app;
        get_args.key = new bamboo_key ();
        get_args.key.value = bi2bytes (key);
        get_args.maxvals = Integer.MAX_VALUE;
        get_args.placemark = new bamboo_placemark ();
        get_args.placemark.value = new byte [] {};

	state.current_get = 0;
        Object [] pair = {state, get_args};

        client.get (get_args, new JoinGetDoneCb (), pair);
    }

    public class JoinGetDoneCb implements GatewayClient.GetDoneCb {

        public void get_done_cb (bamboo_get_res get_res, Object user_data) {
            Object [] pair = (Object []) user_data;
            JoinState state = (JoinState) pair [0];

            for (int i = 0; i < get_res.values.length; ++i) {

                BigInteger hash = 
                    bytes2bi (digest.digest (get_res.values [i].value));
		
                if (logger.isDebugEnabled())
                    logger.debug ("get(" + state.current_level + ") found 0x" 
                                  + GuidTools.guid_to_string (hash));

                InetSocketAddress addr = bytes2addr (get_res.values [i].value);
		NodeId dest = new NodeId(addr.getPort(), addr.getAddress());

		// ignore if node finds itself
		if (my_key.compareTo(hash) == 0) {
		    continue;
		}

		/* for join replication, only count if node is
		    in the same partition immediately under this one
		*/
                if (same_partition(my_key, hash, state.current_level + 1)) {
		    state.current_get++;
		}

		/* for multicast tree, 
		   send out ping messages if node is at:
		     1. the same partition at lowest level, or
		     2. the opposite partition at other levels 
		*/
		if (((state.current_level == (total_levels - 1)) &&
		     (same_partition(my_key, hash, state.current_level))) ||
		    ((state.current_level != (total_levels - 1)) &&
		     (opposite_partition(my_key, hash, state.current_level)))) {

		    //ping this node
		    MulticastPingMessage mpm = new MulticastPingMessage(dest);
                    network.send(mpm, dest, 10, curry(pingDone, dest, 
                                 new Integer(state.current_level)));
		}
            }

            if (get_res.values.length == 0 
                || get_res.placemark.value.length == 0) {

                if (logger.isDebugEnabled())
                    logger.debug ("get(" + state.current_level + ") returned " 
                                  + state.current_get + " nodes.");

		/* Always put if at lowest level */
		/* Always put if level has not reached replication nodes */
		if ((state.current_level == total_levels - 1) ||
		    (state.current_get < replication)) {
		    join_put(state);
		    return;
		}

		/* Didn't put...
		     1. At the highest level, tree done building, so callback
		     2. At other levels, continue to build the tree
		*/
		if (state.current_level == 0) {
		    state.cb.timer_cb (new Integer(state.done_level));
		} else {
		    --state.current_level;
		    join_get(state);
		} 
		return;
            }

            bamboo_get_args get_args = (bamboo_get_args) pair [1];
            get_args.placemark = get_res.placemark;
            client.get (get_args, this, pair);
        }
    }

    Thunk3<NodeId, Integer,Boolean> pingDone = 
    new Thunk3<NodeId,Integer,Boolean>() {
        public void run(NodeId dest, Integer level, Boolean success) {
            if (success.booleanValue())
                dispatch_lat_req(dest, level.intValue());
            else
                remove_from_queue(dest, level.intValue());
        }
    };

    protected void join_put (JoinState state) {
        BigInteger key = rendezvous_point (
                my_key, state.groupname, state.current_level);

        if (logger.isDebugEnabled())
            logger.debug ("doing join_put(" + state.current_level 
                          + ") at 0x" + GuidTools.guid_to_string (key));
	
        bamboo_put_args put = new bamboo_put_args ();
        put.application = state.app;
        put.value = new bamboo_value ();
        put.value.value = my_addr_bytes;
        put.key = new bamboo_key ();
        put.key.value = bi2bytes (key);
        put.ttl_sec = (state.ttl_s);

        client.put (put, new JoinPutDoneCb (), state);
    }

    public class JoinPutDoneCb implements GatewayClient.PutDoneCb {
        public void put_done_cb (int put_res, Object user_data) {
            JoinState state = (JoinState) user_data;
            assert put_res == 0;

	    state.done_level = state.current_level;

            if (state.current_level == 0) {
		//done joining all levels
                state.cb.timer_cb (new Integer(0));
            }
            else {
		--state.current_level;
                join_get (state);
            }
        }
    }

    public void sendMsg(QuickSerializable payload) {
	int num_forwarded = 0;
	for (int i=0; i < total_levels; i++) {
	    num_forwarded += dispatch_send(i, payload);
	}

        if (logger.isDebugEnabled())
            logger.debug("Forwarded to " + num_forwarded + " nodes.");
    }

    public int dispatch_send(int level, QuickSerializable payload) {

	if (logger.isDebugEnabled ()) 
	    logger.debug("Dispatch_send[" + level + "]: " + queue[level]);
	
	int num_forwarded = 0;

	if (level == total_levels-1) {
	    //At lowest level, send to everyone
	    for (int i=1; i <= queue[level].size(); i++) {
		long rtt = queue[level].getIndexPriority(i);
		NodeId peer = (NodeId)queue[level].getIndex(i);

		//send to addr
		/* Since it's sending at the lowest level,
		     no need to get a network response or resend 
		*/
		MulticastMessage sm = new MulticastMessage(groupname, 
                        my_node_id, rtt, timer_ms(), payload);
		sm.add_rtt(rtt/2);
		sm.add_transit(timer_ms());
                network.send(sm, peer);
		num_forwarded++;

		if (logger.isDebugEnabled ()) logger.debug ("Sending("
                        + level + ") to " + peer.address().getHostAddress()
                        + ":" + peer.port ());
	    }
	    
	} else {
	    //For all other levels, send to fastest peer
	    if (!queue[level].isEmpty()) {

		NodeId peer = 
		    (NodeId)(queue[level].getFirst());
		long fastest_rtt = 
		    queue[level].getFirstPriority();
		
		assert(fastest_rtt != -1);
		
		//send to addr
		MulticastMessage sm = new MulticastMessage(groupname,
                        my_node_id, fastest_rtt, timer_ms(), payload);
                MulticastMessage clone = null;
		try { clone = (MulticastMessage) sm.clone(); } 
                catch (CloneNotSupportedException e) { assert false : e; }

		sm.add_rtt(fastest_rtt/2);
		sm.add_transit(timer_ms());

                network.send(sm, peer, curry(resend, new Integer(level), 
                            clone, new Boolean(true)));
		num_forwarded++;
		
		if (logger.isDebugEnabled ())
		    logger.debug ("Sending(" + level + ") to "
				  + peer.address().getHostAddress()
				  + ":" + peer.port ());
	    }
	}
	return num_forwarded;
    }

    protected Thunk4<Integer,MulticastMessage,Boolean,Boolean> resend = 
    new Thunk4<Integer,MulticastMessage,Boolean,Boolean>() {
        public void run(Integer lev, MulticastMessage msg, Boolean sending, 
                        Boolean success) {
            if (!success.booleanValue()) {
                int level = lev.intValue();

                /* Throw away failed node */
                if (!queue[level].isEmpty()) {
                    queue[level].removeFirst();

                    /* Resend to next on the list */
                    if (sending.booleanValue())
                        dispatch_send(level, msg.payload);
                    else
                        dispatch_forward(level, msg);
                }
            }
        }
    };

    public void receive(MulticastMessage msg, InetSocketAddress peer) {

	msg.hops++;

        byte[] recv_bytes = addr2bytes (peer);
        BigInteger recv_key = bytes2bi (digest.digest (recv_bytes));
	
	int level = get_recv_level(my_key, recv_key, total_levels);
	
	int num_forwarded = 0;
	
	if (level != (total_levels - 1)) {
	    // start forward at level + 1
	    for (int i = level + 1; i < total_levels; i++) {

		try {
		    num_forwarded += 
			dispatch_forward(i, (MulticastMessage) msg.clone());
		} catch (CloneNotSupportedException e) {
		    logger.warn ("MulticastMessage exception: ", e);
		}
	    }
	}

        if (logger.isDebugEnabled())
            logger.debug("Received message from " 
                         + msg.sender_id.address().getHostAddress() + ":" 
                         + msg.sender_id.port() + "; est_rtt/2 = " 
                         + (msg.est_rtt/2) + " ms; transit time = " 
                         + (timer_ms() - msg.begin_time) 
                         + " ms; hops = " + msg.hops + ".");
        if (logger.isDebugEnabled())
            logger.debug("Forwarded to " + num_forwarded + " nodes.");

    }

    public int dispatch_forward(int level, MulticastMessage sm) {
	if (logger.isDebugEnabled ()) 
	    logger.debug("Dispatch_forward[" + level + "]: " + queue[level]);

	int num_forwarded = 0;

	if (level == total_levels-1) {
	    //At lowest level, send to everyone
	    for (int i=1; i <= queue[level].size(); i++) {

		MulticastMessage newmsg = sm;

		try { newmsg = (MulticastMessage) sm.clone(); } 
                catch (CloneNotSupportedException e) { assert false; }

		long rtt = queue[level].getIndexPriority(i);
		NodeId peer = (NodeId)queue[level].getIndex(i);

		// update statistics
		newmsg.est_rtt += rtt;
		//sm.last_agg_time = now_ms();
		newmsg.add_rtt(rtt/2);
		newmsg.add_transit(timer_ms());

		//send to addr
		network.send(newmsg, peer);
		num_forwarded++;

		if (logger.isDebugEnabled ())
		    logger.debug ("Forwarding(" + level + ") to "
				  + peer.address().getHostAddress()
                                  + ":" + peer.port ());
	    }
	    
	} else {
	    //For all other levels, send to fastest peer
	    if (!queue[level].isEmpty()) {
		
		MulticastMessage newmsg = sm;
		try { newmsg = (MulticastMessage) sm.clone(); } 
                catch (CloneNotSupportedException e) { assert false; }
		
		NodeId peer = (NodeId)(queue[level].getFirst());
		long fastest_rtt = queue[level].getFirstPriority();
		
		assert(fastest_rtt != -1);

                MulticastMessage clone = null;
		try { clone = (MulticastMessage) sm.clone(); } 
                catch (CloneNotSupportedException e) { assert false; }

		// update statistics
		newmsg.est_rtt += fastest_rtt;
		//sm.last_agg_time = now_ms();

		newmsg.add_rtt(fastest_rtt/2);
		newmsg.add_transit(timer_ms());

		//send to addr
                network.send(newmsg, peer, curry(resend, 
                            new Integer(level), clone, new Boolean(false)));

		num_forwarded++;

		if (logger.isDebugEnabled ())
		    logger.debug ("Forwarding(" + level + ") to "
                                  + peer.address().getHostAddress()
                                  + ":" + peer.port ());
	    }
	}
	return num_forwarded;
    }

    public void dispatch_lat_req(NodeId peer, int level) {
        if (logger.isDebugEnabled())
            logger.debug("peer=" + peer);
        long rtt = network.estimatedRTTMillis(peer);
        assert rtt != -1;
            handle_lat_resp(peer, level, rtt);
    }

    public void remove_from_queue(NodeId peer, int level) {
	PriorityQueue tmp_queue = new PriorityQueue(10);
	for (int i = 1; i <= queue[level].size(); i++) {
	    NodeId nid = (NodeId) queue[level].getIndex(i);
	    if (!nid.equals(peer)) {
		tmp_queue.add(nid, queue[level].getIndexPriority(i));
	    }
	}
	queue[level] = tmp_queue;

	if (logger.isDebugEnabled ())
	    logger.debug("Removing node: " + peer + "; queue[" + level 
                         + "]=" + queue[level]);

    }

    public void handle_lat_resp(NodeId peer, int level, long rtt_ms) {
	assert peer != my_node_id;

	if (queue[level].contains(peer)) {
	    PriorityQueue tmp_queue = new PriorityQueue(10);
	    for (int i = 1; i <= queue[level].size(); i++) {
		NodeId nid = (NodeId) queue[level].getIndex(i);
		if (nid.equals(peer)) {
		    tmp_queue.add(peer, rtt_ms);
		} else {
		    tmp_queue.add(nid, queue[level].getIndexPriority(i));
		}
	    }
	    queue[level] = tmp_queue;
	} else {
	    queue[level].add(peer, rtt_ms);
	}
    }
}

