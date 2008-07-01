/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dmgr;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.LinkedHashMap;
import ostore.util.NodeId;
import ostore.util.Pair;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import seda.sandStorm.api.StagesInitializedSignal;
import bamboo.api.BambooLeafSetChanged;
import bamboo.api.BambooNeighborInfo;
import bamboo.api.BambooRouteDeliver;
import bamboo.api.BambooRouteInit;
import bamboo.api.BambooRouterAppRegReq;
import bamboo.api.BambooRouterAppRegResp;
import bamboo.db.StorageManager;
import bamboo.lss.ASyncCore;
import bamboo.util.GuidTools;
import java.net.InetAddress;
import ostore.util.ByteUtils;
import static bamboo.db.StorageManager.Key;
import static bamboo.util.Curry.*;
import static bamboo.util.GuidTools.guid_to_string;
import static bamboo.util.StringUtil.*;
import static java.lang.Math.ceil;
import static java.lang.Math.log;
import static java.lang.Math.min;
import static java.lang.Math.round;

/**
 * Manages the data stored on Bamboo nodes.
 *
 * @author Sean C. Rhea
 * @version $Id: DataManager.java,v 1.70 2005/07/08 21:39:50 srhea Exp $
 */
public class DataManager extends bamboo.util.StandardStage
implements SingleThreadedEventHandlerIF {

    protected static LinkedHashMap<InetSocketAddress,DataManager> instances =
        new LinkedHashMap<InetSocketAddress,DataManager>();

    public static DataManager instance(InetSocketAddress addr) {
        return instances.get(addr);
    }

    //////////////////////////// GENERAL STATE //////////////////////////////

    protected static final BigInteger MIN_GUID = BigInteger.valueOf (0);
    protected static final BigInteger NEG_ONE =
        BigInteger.ZERO.subtract (BigInteger.ONE);

    protected static final long app_id =
        bamboo.router.Router.applicationID(DataManager.class);

    protected BigInteger MODULUS;   // These are both constant once
    protected BigInteger MAX_GUID;  // they are set.

    protected Random rand;
    protected BigInteger my_guid;
    protected boolean initialized;
    protected LinkedList wait_q = new LinkedList ();
    protected BambooNeighborInfo [] preds;
    protected BambooNeighborInfo [] succs;
    protected MessageDigest md;
    protected int ae_period;
    protected int required_acks;
    protected long put_retry_time;
    protected long put_give_up_time;
    protected int desired_replicas;

    /**
     * Whether or not we do iterative routing for ReplicaSetReq messages.
     */
    protected boolean iterative_routing;

    /**
     * We are responsible for storing pointers in the ranges
     * [resp_low, my_guid] and [my_guid, resp_high]
     */
    protected BigInteger resp_low, resp_high;

    protected BambooNeighborInfo random_ls_member () {

	// TODO: weight by distance

	if (preds == null) {
	    if (logger.isDebugEnabled ())
                logger.debug ("random_ls_member: no preds");
	    return null;
	}
	if (preds.length == 0) {
	    if (logger.isDebugEnabled ())
                logger.debug ("random_ls_member: empty preds");
	    return null;
	}

	// Find successors which are not also predecessors.

	int which = preds.length;
	for (int i = 0; i < succs.length; ++i) {
	    if (succs [i].equals (preds [preds.length - 1]))
		break;
	    ++which;
	}

	which = rand.nextInt (which);
	if (which < preds.length) {
	    if (logger.isDebugEnabled ()) logger.debug (
		    "random_ls_member: chose " + preds [which]);
	    return preds [which];
	}

	which -= preds.length;

	if (logger.isDebugEnabled ()) logger.debug (
		"random_ls_member: chose " + succs [which]);
	return succs [which];
    }

    public DataManager () throws Exception {

	ostore.util.TypeTable.register_type (ReplicaSetReq.class);

	event_types = new Class [] {
	    StagesInitializedSignal.class,
	    PutOrRemoveReq.class,
            AntiEntropyAlarm.class,
            DiscardAlarm.class,
            BambooRouteDeliver.class
	};

	inb_msg_types = new Class [] {
	    PutOrRemoveMsg.class,
	    PutOrRemoveAck.class,
	    ReplicaSetResp.class,
	    PutOrRemoveMsg.class,
	    FetchMerkleTreeNodeReq.class,
	    FetchMerkleTreeNodeResp.class,
	    FetchMerkleTreeNodeReject.class,
	    FetchKeysReq.class,
	    FetchKeysResp.class,
	    FetchDataReq.class,
	    FetchDataResp.class
	};

        try { md = MessageDigest.getInstance ("SHA"); }
        catch (Exception e) { BUG (e); }
    }

    public void init (ConfigDataIF config) throws Exception {

	super.init (config);

        instances.put(my_node_id, this);

        desired_replicas = configGetInt(config, "desired_replicas",
                                        Integer.MAX_VALUE - 1); // so it's even
        assert (desired_replicas & 0x1) == 0;

	iterative_routing = config_get_boolean (config, "iterative_routing");
	expansion = config_get_int (config, "merkle_tree_expansion");
        if (expansion <= 0)
            expansion = 5;

	ae_period = config_get_int (config, "ae_period");
	if (ae_period == -1)
	    ae_period = 1;
	ae_period *= 1000;

        required_acks = config_get_int (config, "required_acks");
        if (required_acks == -1)
            required_acks = Integer.MAX_VALUE;

        put_retry_time = config_get_int (config, "put_retry_time");
        if (put_retry_time == -1)
            put_retry_time = 60*1000;

        // By default, put_give_up_time == put_retry_time, so we never retry.
        put_give_up_time = config_get_int (config, "put_give_up_time");
        if (put_give_up_time == -1)
            put_give_up_time = 60*1000;
    }

    public void handleEvent (QueueElementIF item) {
	if (logger.isDebugEnabled ())
	    logger.debug (" got " + item);

	if (item instanceof StagesInitializedSignal) {
	    dispatch (new BambooRouterAppRegReq (
			app_id, true, false, false, my_sink));
	}
	else if (item instanceof BambooRouterAppRegResp) {
            handle_app_reg_resp ((BambooRouterAppRegResp) item);
	}
	else if (! initialized) {
	    wait_q.addLast (item);
	}
	else {
	    if (item instanceof BambooLeafSetChanged) {
		handle_leaf_set_changed ((BambooLeafSetChanged) item);
	    }
	    else if (item instanceof PutOrRemoveMsg) {
		handle_pub_to_leaf_set ((PutOrRemoveMsg) item);
	    }
	    else if (item instanceof PutOrRemoveAck) {
		handle_pub_to_leaf_set_ack ((PutOrRemoveAck) item);
	    }
	    else if (item instanceof PutOrRemoveReq) {
		handle_put_or_remove_req ((PutOrRemoveReq) item);
	    }
	    else if (item instanceof StorageManager.PutResp) {
		handle_put_resp ((StorageManager.PutResp) item);
	    }
	    else if (item instanceof StorageManager.GetByKeyResp) {
                handle_get_by_key_resp ((StorageManager.GetByKeyResp) item);
            }
	    else if (item instanceof StorageManager.GetByGuidResp) {
                StorageManager.GetByGuidResp resp =
                    (StorageManager.GetByGuidResp) item;
		handle_discard_get_resp (resp);
            }
	    else if (item instanceof StorageManager.GetByTimeResp) {
                 handle_get_by_time_resp ((StorageManager.GetByTimeResp) item);
            }
	    else if (item instanceof AntiEntropyAlarm) {
                 handle_ae_alarm ((AntiEntropyAlarm) item);
            }
	    else if (item instanceof DiscardAlarm) {
                 handle_discard_alarm ((DiscardAlarm) item);
            }
	    else if (item instanceof FetchMerkleTreeNodeReq) {
                 handle_fetch_merkle_tree_node_req (
                         (FetchMerkleTreeNodeReq) item);
            }
	    else if (item instanceof FetchMerkleTreeNodeResp) {
                 handle_fetch_merkle_tree_node_resp (
                         (FetchMerkleTreeNodeResp) item);
            }
	    else if (item instanceof FetchMerkleTreeNodeReject) {
                 handle_fetch_merkle_tree_node_reject (
                         (FetchMerkleTreeNodeReject) item);
            }
            else if (item instanceof FetchKeysReq) {
                handle_fetch_keys_req ((FetchKeysReq) item);
            }
	    else if (item instanceof FetchKeysResp) {
                handle_fetch_keys_resp ((FetchKeysResp) item);
            }
            else if (item instanceof FetchDataReq) {
                handle_fetch_data_req ((FetchDataReq) item);
            }
	    else if (item instanceof FetchDataResp) {
                handle_fetch_data_resp ((FetchDataResp) item);
            }
	    else if (item instanceof BambooRouteDeliver) {
                BambooRouteDeliver deliver = (BambooRouteDeliver) item;
                handle_replica_set_req ((ReplicaSetReq) deliver.payload);
            }
	    else if (item instanceof ReplicaSetResp) {
                handle_replica_set_resp ((ReplicaSetResp) item);
            }
            else {
		BUG ("got event of unexpected type: " + item + ".");
	    }
	}
    }

    protected void handle_app_reg_resp (BambooRouterAppRegResp resp) {
        my_guid = resp.node_guid;
        MODULUS = resp.modulus;
        MAX_GUID = MODULUS.subtract (BigInteger.ONE);
        my_neighbor_info = new BambooNeighborInfo (my_node_id, my_guid, 0.0);
        rand = new Random (my_guid.longValue ());
        initialized = true;

        while (! wait_q.isEmpty ())
            handleEvent ((QueueElementIF) wait_q.removeFirst ());

        classifier.dispatch_later (new AntiEntropyAlarm (),
                ae_period + rand.nextInt (ae_period));
        classifier.dispatch_later (new DiscardAlarm (true),
                5000 + rand.nextInt (5000));
        next_check_put_or_remove_acks ();
    }

    protected boolean in_leaf_set_range (BigInteger q) {
	return (resp_low == null) ||
	    in_range_mod (resp_low, my_guid, q) ||
	    in_range_mod (my_guid, resp_high, q);
    }

    public int desiredReplicas() {
        return desired_replicas;
    }

    protected void handle_leaf_set_changed (BambooLeafSetChanged msg) {

	if (msg.preds.length == 0) {

	    // We can only have no predecessors if we are the only node in
	    // the network.  Go back to our original state.

	    preds = null;
	    succs = null;
            resp_low = MIN_GUID;
            resp_high = MAX_GUID;
            merkle_trees = new TreeMap ();
	    return;
	}

        // We only want to know about the closest desired_replicas / 2 on each
        // side.
         
        assert msg.preds.length == msg.succs.length : 
            "\n" + bamboo.router.Router.instance(my_node_id).leafSet();
        int len = min(msg.preds.length, desired_replicas / 2);

	// See if the new leaf set covers all nodes.  This only happens if
	// one of the predecessors is also the furthest successor.

	boolean new_ls_covers_all = false;
	for (int i = 0; i < len; ++i) {
	    if (msg.preds [i].equals (msg.succs [len - 1])) {
		new_ls_covers_all = true;
		break;
	    }
	}

	// Update our responsibilities and leaf set.

	if (new_ls_covers_all) {
            resp_low = MIN_GUID;
            resp_high = MAX_GUID;
	}
	else {
	    resp_low = msg.preds [len - 1].guid;
	    resp_high = msg.succs [len - 1].guid.subtract (BigInteger.ONE);
            if (resp_high.equals (NEG_ONE))
                resp_high = MAX_GUID;
	}

        preds = new BambooNeighborInfo[len];
        succs = new BambooNeighborInfo[len];
        for (int i = 0; i < len; ++i) {
            preds[i] = msg.preds[i];
            succs[i] = msg.succs[i];
        }

	if (logger.isDebugEnabled()) {
            logger.debug("responsibility is [" 
                         + GuidTools.guid_to_string(resp_low) + ", " 
                         + GuidTools.guid_to_string(resp_high) + "]");
        }

        // Get rid of MerkleTrees for databases we're no longer responsible
        // for.

        SortedSet db_ranges = new TreeSet ();
        if (new_ls_covers_all) {
            GuidRange range = new GuidRange (MIN_GUID, MAX_GUID);
            db_ranges.add (range);
        }
        else {
            for (int i = -1 * preds.length; i < succs.length; ++i) {
                BambooNeighborInfo one = null, two = null;

                if (i == 0)
                    one = my_neighbor_info;
                else if (i < 0)
                    one = preds [-1 * i - 1];
                else
                    one = succs [i - 1];

                if (i+1 == 0)
                    two = my_neighbor_info;
                else if (i+1 < 0)
                    two = preds [-1 * (i+1) - 1];
                else
                    two = succs [(i+1) - 1];

                BigInteger second = two.guid.subtract (BigInteger.ONE);
                if (second.equals (NEG_ONE))
                    second = MAX_GUID;
                GuidRange range = new GuidRange (one.guid, second);
                if (logger.isDebugEnabled ()) logger.debug ("share range [" +
                        GuidTools.guid_to_string (one.guid) + ", " +
                        GuidTools.guid_to_string (second) + "]");

                db_ranges.add (range);
            }
        }

        for (Iterator i = merkle_trees.keySet ().iterator (); i.hasNext (); ) {
            GuidRange r = (GuidRange) i.next ();
            if (! db_ranges.contains (r)) {
                if (logger.isDebugEnabled ())
                    logger.debug ("discarding db " + r);
                i.remove ();
            }
        }

        for (Iterator<GuidRange> i = synced_ranges.keySet().iterator(); 
             i.hasNext();) {
            GuidRange range = i.next();
            if (! db_ranges.contains (range)) {
                if (logger.isDebugEnabled ())
                    logger.debug("no longer responsible for " + range);
                i.remove();
            }
        }
    }

    protected boolean in_range_mod (
	    BigInteger low, BigInteger high, BigInteger query) {
	return GuidTools.in_range_mod (low, high, query, MODULUS);
    }

    /////////////////////////////////////////////////////////////////////////
    //
    //                         PUT/REMOVE STATE
    //
    // These data members and functions handle the first time we see a put or
    // remove, either because we are the root and we get a put/remove from
    // another local stage, or because we get a PutOrRemoveMsg message from
    // the remote root.
    //
    /////////////////////////////////////////////////////////////////////////

    protected Map put_or_remove_acks = new TreeMap ();
    protected long next_put_or_remove_seq = 0;
    protected static class PutOrRemoveState {
	public long start_time;
        public boolean stored;
        public int expected_ack_cnt;
	public Set unacked;
	public PutOrRemoveReq event;
        public byte [] hash;
	public PutOrRemoveState(long s, Set u, PutOrRemoveReq e, byte [] h) {
	    start_time = s; unacked = u; event = e; hash = h;
            expected_ack_cnt = (u == null) ? 0 : u.size ();
        }
    }

    protected void handle_put_or_remove_req (PutOrRemoveReq e) {

        if (logger.isDebugEnabled ())
            logger.debug ("root, sending to leaf set");

        Set pa = new TreeSet ();

        if (preds != null) {
            // If the guid isn't between us and our immediate predecessor, 
            // we don't forward to the last of our predecessors.
            int pred_max = preds.length - 1;
            if (! in_range_mod (preds [0].guid, my_guid, e.guid)) {
                --pred_max;
            }
            for (int i = pred_max; i >= 0; --i) {
                if (logger.isDebugEnabled ())
                    logger.debug ("sending to " + preds [i].node_id);
                pa.add (preds [i].node_id);
            }
        }
        if (succs != null) {
            // If the guid isn't between us and our immediate successor, 
            // we don't forward to the last of our successors.
            int succ_max = succs.length;
            if (! in_range_mod (my_guid, succs [0].guid, e.guid)) {
                --succ_max;
            }
            for (int i = 0; i < succ_max; ++i) {
                if (logger.isDebugEnabled ())
                    logger.debug ("sending to " + succs [i].node_id);
                pa.add (succs [i].node_id);
            }
        }

        byte [] hash = e.value_hash;
        if (e.put) {
            md.update(e.value.array(), e.value.arrayOffset(), e.value.limit());
            hash = md.digest ();
        }

        Long seq = new Long (next_put_or_remove_seq++);
        put_or_remove_acks.put(
                seq, new PutOrRemoveState(timer_ms(), pa, e, hash));

        Key key = 
            new Key(e.time_usec, e.ttl_sec, e.guid, e.secret_hash, hash, 
                    e.put, e.client_id);

        if (!key.put) {
            byte [] secretHash = md.digest(e.value.array());
            if (!Arrays.equals(key.secret_hash, secretHash)) {
                StringBuffer buf = new StringBuffer (200);
                buf.append ("got bad remove ");
                key.toStringBuffer(buf);
                buf.append (" hash(data)=0x");
                bytes_to_sbuf(secretHash, 0, 4, buf);
                buf.append (" as root");
                logger.warn(buf);
                return;
            }
        }

        if (logger.isInfoEnabled ()) {
            StringBuffer buf = new StringBuffer (200);
            buf.append ("got ");
            key.toStringBuffer(buf);
            buf.append (" size=");
            buf.append (e.value.limit());
            buf.append (" as root");
            logger.info (buf);
        }

        db_put(key, e.value, seq);

        if (pa != null) {
            Iterator i = pa.iterator ();
            while (i.hasNext ())
                dispatch (new PutOrRemoveMsg ((NodeId) i.next (), e.time_usec,
			  e.ttl_sec, e.guid, e.value, e.put,
			  e.client_id, seq.longValue (), e.value_hash, 
                          e.secret_hash));
        }
    }

    protected void next_check_put_or_remove_acks () {
        long next_time = 30*1000 + rand.nextInt (60*1000);
        acore.register_timer (next_time, check_put_or_remove_acks, null);
    }

    protected ASyncCore.TimerCB check_put_or_remove_acks = 
        new ASyncCore.TimerCB () {
        public void timer_cb (Object user_data) {    
            LinkedList resend = null;
            long timer_ms = timer_ms ();
            Iterator i = put_or_remove_acks.keySet ().iterator ();
            while (i.hasNext ()) {
                Long seq = (Long) i.next ();
                PutOrRemoveState p =
                    (PutOrRemoveState) put_or_remove_acks.get (seq);
                if (p.stored && (timer_ms > p.start_time + put_give_up_time)) {
                    i.remove ();
                    if (p.expected_ack_cnt - p.unacked.size () < required_acks) {
                        logger.warn ("did not get " + required_acks + 
                                " acks for put");
                    }
                    if (p.event.completion_queue != null) {
                        enqueue (p.event.completion_queue,
                                new PutOrRemoveResp (p.event.user_data));
                    }
                }
                else if (timer_ms > p.start_time + put_retry_time) {
                    if (resend == null) resend = new LinkedList ();
                    resend.addLast (new Object [] {seq, p});
                    i.remove ();
                }
            }
            if (resend != null) {
                i = resend.iterator ();
                while (i.hasNext ()) {
                    Object [] o = (Object []) i.next ();
                    Long seq = (Long) o[0];
                    PutOrRemoveState p = (PutOrRemoveState) o[1];
                    p.start_time = timer_ms;
                    // Don't use a new sequence number here.  What if the
                    // local store operation hasn't finished?
                    // long seq = next_put_or_remove_seq++;
                    put_or_remove_acks.put (seq, p);
                    Iterator j = p.unacked.iterator ();
                    while (j.hasNext ()) {
                        dispatch (new PutOrRemoveMsg ((NodeId) j.next (),
                                    p.event.time_usec, p.event.ttl_sec, 
                                    p.event.guid, p.event.value, p.event.put, 
                                    p.event.client_id, seq.longValue (), 
                                    p.hash, p.event.secret_hash));
                    }
                }
            }
            next_check_put_or_remove_acks (); 
        }
    };

    protected void handle_pub_to_leaf_set (PutOrRemoveMsg msg) {

	if(logger.isDebugEnabled ()) logger.debug (
		"handle_pub_to_leaf_set called w/ msg=" + msg +
		" in_leaf_set_range(guid=" +
		GuidTools.guid_to_string (msg.guid) + ")=" +
		in_leaf_set_range (msg.guid));

        byte [] hash = msg.value_hash;
        if (msg.put) {
            md.update(msg.value.array(), msg.value.arrayOffset(), 
                      msg.value.limit());
            hash = md.digest();
        }

        Key key = new Key(msg.time_usec, msg.ttl_sec, msg.guid,
                          msg.secret_hash, hash, msg.put, msg.client_id);

        if (!key.put) {
            byte [] secretHash = md.digest(msg.value.array());
            if (!Arrays.equals(key.secret_hash, secretHash)) {
                StringBuffer buf = new StringBuffer (200);
                buf.append ("got bad remove ");
                key.toStringBuffer(buf);
                buf.append (" hash(data)=0x");
                bytes_to_sbuf(secretHash, 0, 4, buf);
                buf.append (" from root ");
                addr_to_sbuf(msg.peer, buf);
                logger.warn(buf);
                return;
            }
        }

        if (logger.isInfoEnabled ()) {
            StringBuffer buf = new StringBuffer (200);
            buf.append ("got ");
            key.toStringBuffer(buf);
            buf.append (" size=");
            buf.append (msg.value.limit ());
            buf.append (" from root ");
            addr_to_sbuf(msg.peer, buf);
            logger.info (buf);
        }

        db_put(key, msg.value, msg);
    }

    protected void update_merkle_trees (StorageManager.PutResp resp) {
        if (resp.inval_put_key != null)
            invalidate_merkle_trees (resp.inval_put_key.guid,
                                     resp.inval_put_key.time_usec);
        if (resp.inval_rm_key != null)
            invalidate_merkle_trees (resp.inval_rm_key.guid,
                                     resp.inval_rm_key.time_usec);
    }

    protected void handle_put_resp (StorageManager.PutResp resp) {
        if (resp.user_data instanceof PutOrRemoveMsg) {
            PutOrRemoveMsg msg = (PutOrRemoveMsg) resp.user_data;
            dispatch (new PutOrRemoveAck (msg.peer, msg.seq));
        }
        else if (resp.user_data instanceof Long) {
            Long seq = (Long) resp.user_data;
            PutOrRemoveState p =
		(PutOrRemoveState) put_or_remove_acks.get (seq);
            assert p != null;
            p.stored = true;
            check_put_or_remove_done (seq, p);
        }
        else if (resp.user_data == null) {
            // do nothing
        }
        else {
            assert false : resp.user_data.getClass ().getName ();
        }
        update_merkle_trees (resp);
    }

    protected void handle_pub_to_leaf_set_ack (PutOrRemoveAck msg) {

	if ((ds != null) && (msg.seq == ds.nonce)) {

	    // This is the end of a discard state operation.  Start right
	    // over again, in case there are more.  If there aren't, we'll
	    // dispatch another alarm after a delay from the
	    // handle_discard_get_resp function.

            if (logger.isInfoEnabled ()) {
                StringBuffer buf = new StringBuffer (200);
                buf.append ("discard success: gave ");
                ds.key.toStringBuffer(buf);
                buf.append (" to ");
                addr_to_sbuf(msg.peer, buf);
                logger.info (buf);
            }

            dispatch (new StorageManager.GetByGuidCont (
                        ds.cont, true, // delete last
                        null, null));
	    ds = null;
	    dispatch (new DiscardAlarm (false));

	    return;
	}

	PutOrRemoveState p =
	    (PutOrRemoveState) put_or_remove_acks.get (new Long (msg.seq));
	if (p == null) {
	    if (logger.isDebugEnabled ())
                logger.debug ("unexpected " + msg + ".  Dropping it.");
	    return;
	}
	if (! p.unacked.remove (msg.peer)) {
	    if (logger.isDebugEnabled ())
                logger.debug ("duplicate or unexpected " + msg +
			".  Dropping it.");
	    return;
	}

        check_put_or_remove_done (new Long (msg.seq), p);
    }

    protected void check_put_or_remove_done (Long seq, PutOrRemoveState p) {
        // Send the completion event if we haven't already sent it...
        if ((p.event.completion_queue != null)
                // ... and we've stored the value ourselves...
                && p.stored
                // ... and we've received at least required_acks ...
                && ((p.expected_ack_cnt - p.unacked.size () >= required_acks)
                    // ... or we're not waiting for any more acks, even though
                    // the number we've received is less than required_acks
                    // (b/c our leaf set had less than required_acks members
                    // at the time of the request)
                    || (p.unacked.isEmpty ()))) {

            enqueue (p.event.completion_queue,
                    new PutOrRemoveResp (p.event.user_data));
            p.event.completion_queue = null; // so we don't send it again

            /* System.out.println("got " + (p.expected_ack_cnt - p.unacked.size())
                    + " acks; sending completion event"); */
        }
        if (p.unacked.isEmpty () && p.stored) {
            put_or_remove_acks.remove (seq);
        }
    }

    protected void db_put(Key key, ByteBuffer value, Object user_data) {
        invalidate_merkle_trees (key.guid, key.time_usec);
        StorageManager.PutReq outb = new StorageManager.PutReq (
                key, value, my_sink, user_data);
        dispatch (outb);
    }

    ////////////////////////// MERKLE TREE STATE ////////////////////////////

    protected int expansion;
    protected Map merkle_trees = new TreeMap ();
    protected LinkedHashMap<GuidRange,LinkedHashMap<InetSocketAddress,Long>>
        synced_ranges = 
        new LinkedHashMap<GuidRange,LinkedHashMap<InetSocketAddress,Long>>();

    protected static class GuidRange implements Comparable {
        public BigInteger low, high;
        public GuidRange (BigInteger l, BigInteger h) { low = l; high = h; }
        public boolean equals (Object rhs) {
            GuidRange other = (GuidRange) rhs;
            return low.equals (other.low) && high.equals (other.high);
        }
        public int hashCode () { return low.hashCode () ^ high.hashCode (); }
        public int compareTo (Object rhs) {
            GuidRange other = (GuidRange) rhs;
            int result = low.compareTo (other.low);
            if (result != 0)
                return result;
            else
                return high.compareTo (other.high);
        }
        public String toString () {
            return "[" + GuidTools.guid_to_string (low) + ", " +
                GuidTools.guid_to_string (high) + "]";
        }
    }

    protected SortedSet calc_shared_dbs (BambooNeighborInfo other) {

        if (logger.isDebugEnabled ()) logger.debug ("calc_shared_dbs (" +
                GuidTools.guid_to_string (my_guid) + ", " +
                GuidTools.guid_to_string (other.guid) + ")");

        if (logger.isDebugEnabled ())
            logger.debug ("calc_shared_dbs (" + my_node_id + ", " 
                          + other.node_id + ")");

	// Check and see if the leaf set covers the whole space, and at the
	// same time see if this neighbor is a successor of mine.

	boolean is_succ = false;
	int succ_idx = 0;
	for (int i = 0; i < succs.length; ++i) {
	    if (succs [i].equals (preds [preds.length - 1])) {
                // The leaf set covers the whole space.  Treat it all as one
                // big database.
                if (logger.isDebugEnabled ())
                    logger.debug ("whole space shared");
                GuidRange db_range = new GuidRange (MIN_GUID, MAX_GUID);
                SortedSet result = new TreeSet ();
                result.add (db_range);
                return result;
            }
	    if (succs [i].equals (other)) {
		is_succ = true;
		succ_idx = i;
	    }
	}

        int low_idx = 0;
        int high_idx = 0;

	if (is_succ) {
            // If this neighbor is a successor, the shared range of
            // identifiers is from one of my predecessors or me through the
            // end of my successors.

            if (succ_idx == succs.length - 1)
                low_idx = 0;
            else
                low_idx = -1 * (preds.length - succ_idx - 1);
            high_idx = succs.length;
	}
        else {

            // If this neighbor is a predecessor, the shared range of
            // identifiers is from the end of my predecessors through one of
            // my successors or me.

            int pred_idx = -1;
            for (int i = 0; i < preds.length; ++i) {
                if (preds [i].equals (other)) {
                    pred_idx = i;
                    break;
                }
            }
            if (pred_idx == -1) {
                // "other" is neither one of our predecessors nor successors,
                // so we have no shared databases.
                return null;
            }

            if (pred_idx == preds.length - 1)
                high_idx = 0;
            else
                high_idx = (succs.length - pred_idx - 1);
            low_idx = -1 * preds.length;
        }

        SortedSet result = new TreeSet ();
        for (int i = low_idx; i < high_idx; ++i) {
            BambooNeighborInfo one = null, two = null;

            if (i == 0)
                one = my_neighbor_info;
            else if (i < 0)
                one = preds [-1 * i - 1];
            else
                one = succs [i - 1];

            if (i+1 == 0)
                two = my_neighbor_info;
            else if (i+1 < 0)
                two = preds [-1 * (i+1) - 1];
            else
                two = succs [(i+1) - 1];

            BigInteger second = two.guid.subtract (BigInteger.ONE);
            if (second.equals (NEG_ONE))
                second = MAX_GUID;
            GuidRange db_range = new GuidRange (one.guid, second);
            if (logger.isDebugEnabled ()) logger.debug ("share range [" +
                GuidTools.guid_to_string (one.guid) + ", " +
                GuidTools.guid_to_string (second) + "]");

            result.add (db_range);
        }

	return result;
    }

    /**
     * If a piece of code needs the Merkle tree stored by a
     * <code>TreeState</code> object to be valid from the root down, it calls
     * <code>TreeState.wait_for_ready</code> with a callback of this type;
     * when the tree is valid, the <code>TreeState</code> object will call all
     * of the registered callbacks.
     */
    protected interface TreeReadyCB {
        void tree_ready (MerkleTree tree);
    }

    protected class TreeState {
        public MerkleTree tree;
        public LinkedList waiting = new LinkedList ();
        public MerkleTree.FillHolesState fhs;
        public LinkedList keys = new LinkedList ();
        public BigInteger low_guid, high_guid;

        public TreeState (BigInteger lg, BigInteger hg) {
            low_guid = lg; high_guid = hg;
            tree = new MerkleTree (expansion, md);
        }

        public void wait_for_ready (TreeReadyCB cb) {
            waiting.addLast (cb);
            if (waiting.size () == 1) {
                fhs = tree.root ().fill_holes (null, now_ms ());
                handle_fhs (tree);
            }
        }

        protected void handle_fhs (MerkleTree tree) {
            if (fhs == null) {
                Iterator i = waiting.iterator ();
                // Clear waiting list in case one of these callbacks calls
                // wait_for_ready.
                waiting = new LinkedList ();
                while (i.hasNext ()) {
                    TreeReadyCB cb = (TreeReadyCB) i.next ();
                    cb.tree_ready (tree);
                }
            }
            else {
                assert ! waiting.isEmpty () : "fhs=" + fhs;
                keys = new LinkedList ();
                StorageManager.GetByTimeReq req =
                    new StorageManager.GetByTimeReq (
                            fhs.range_low, fhs.range_high, my_sink, this);
                dispatch (req);
            }
        }

        protected void handle_key (StorageManager.GetByTimeResp resp) {

            assert ! waiting.isEmpty () : "fhs=" + fhs;

            GuidRange p = new GuidRange (low_guid, high_guid);
            if ((! merkle_trees.containsKey (p) ||
                (merkle_trees.get(p) != this))) {
                return; // this tree is no longer interesting
            }

            if (resp.continuation == null) {
                // All read.
                if (logger.isDebugEnabled ()) logger.debug ("all keys read");
                fhs.leaves_below = keys.size ();
                Iterator i = keys.iterator ();
		long earliest_exp_usec = Long.MAX_VALUE;
                while (i.hasNext ()) {
                    StorageManager.Key k = (StorageManager.Key) i.next ();
                    byte [] bytes = new byte [StorageManager.Key.SIZE];
                    k.to_byte_buffer (ByteBuffer.wrap (bytes, 0, bytes.length));
		    long e = k.expiryTime();
		    if (e < earliest_exp_usec)
			earliest_exp_usec = e;
                    md.update (bytes);
                }
                fhs.digest = md.digest ();
		fhs.earliest_expiry_usec = earliest_exp_usec;
                fhs = tree.root ().fill_holes (fhs, now_ms ());
                handle_fhs (tree);
            }
            else {
                // There are more keys to read.

                // First add these keys.
                for (StorageManager.Key k : resp.keys) {
                    if (in_range_mod (low_guid, high_guid, k.guid)) {
                        keys.addLast (k);
                    }
                }

                // Fails with fhs=null, keys.size=13, waiting.size=0,
                // resp.cont=StorageManager$GBTCont, low_guid=0,
                // high_guid=0xffffffffffffffffffffffffffffffffffffffff

                if (keys.size () > fhs.max_leaves_below) {
                    // Stop the scan.
                    dispatch (new StorageManager.GetByTimeCont (
                                resp.continuation, null, null));

                    if (logger.isDebugEnabled ())
                        logger.debug ("too many keys: size=" +
                            keys.size () + " max=" + fhs.max_leaves_below);
                    fhs.leaves_below = keys.size ();
                    fhs.digest = null;
                    fhs = tree.root ().fill_holes (fhs, now_ms ());
                    handle_fhs (tree);
                }
                else {
                    if (logger.isDebugEnabled ())
                        logger.debug ("fetching next key");
                    StorageManager.GetByTimeCont cont =
                        new StorageManager.GetByTimeCont (
                                resp.continuation, my_sink, this);
                    dispatch (cont);
                }
            }
        }
    }

    protected void handle_get_by_time_resp (StorageManager.GetByTimeResp resp) {

        // Check to see if we need to get rid of this tuple.
        if (resp.continuation != null) {
            for (StorageManager.Key k : resp.keys) {
                if (! in_leaf_set_range (k.guid)) 
                    dispatch (new StorageManager.DiscardReq (k, true));
            }
        }

        if (resp.user_data instanceof TreeState) {
            TreeState ts = (TreeState) resp.user_data;
            ts.handle_key (resp);
        }
        else {
            handle_fetch_keys_get_resp (resp);
        }
    }

    protected TreeState tree_state (BigInteger low_guid, BigInteger high_guid) {
        GuidRange p = new GuidRange (low_guid, high_guid);
        if (merkle_trees.containsKey (p))
            return (TreeState) merkle_trees.get (p);
        TreeState result = new TreeState (low_guid, high_guid);
        merkle_trees.put (p, result);
        return result;
    }

    protected void invalidate_merkle_trees (BigInteger guid, long time_usec) {
        Iterator i = merkle_trees.keySet ().iterator ();
        while (i.hasNext ()) {
            GuidRange p = (GuidRange) i.next ();
            if (in_range_mod (p.low, p.high, guid)) {
                if (logger.isDebugEnabled ())
                    logger.debug ("invalidating tree");
                TreeState ts = (TreeState) merkle_trees.get (p);
                ts.tree.root ().invalidate_path (time_usec);
            }
        }
    }

    //////////////////// CLIENT-SIDE ANTI_ENTROPY STATE /////////////////////

    protected static class AntiEntropyAlarm implements QueueElementIF {}
    protected long next_fetch_seq;
    protected long next_fetch_keys_seq;
    protected AntiEntropyState ae_state;
    protected static class AntiEntropyState {
        public BambooNeighborInfo ni;
        public BigInteger low_guid, high_guid;
        public GuidRange guid_range;
        public int current_level;
        public long current_low_time;
        public long fetch_node_seq;
        public long last_activity_ms;
        public long fetch_keys_seq;
        public Set fetched_keys = new TreeSet ();
        public Set fetched_data = new TreeSet ();
        public LinkedList nodes_to_do = new LinkedList ();
        public String toString () { return "AE-" + hashCode (); }
    }

    protected void choose_db (BambooNeighborInfo other) {
        SortedSet choices = calc_shared_dbs (other);
        int which = rand.nextInt (choices.size ());
        int orig = which;
        Iterator i = choices.iterator ();
        while (which-- > 0)
            i.next ();
        GuidRange range = (GuidRange) i.next ();
        ae_state.low_guid = range.low;
        ae_state.high_guid = range.high;
        ae_state.guid_range = range;
        if (logger.isDebugEnabled ())
            logger.debug ("which=" + orig + " chose range [" +
                GuidTools.guid_to_string (range.low) + ", " +
                GuidTools.guid_to_string (range.high) + "]");
    }

    public boolean synced(BigInteger key, InetSocketAddress neighbor) {
        assert preds != null;
        for (GuidRange range : synced_ranges.keySet()) {
            if ((key.compareTo(range.low) >= 0) 
                 && (key.compareTo(range.high) <= 0)) {
                return synced_ranges.get(range).containsKey(neighbor);
            }
        }
        return false;
    }

    public boolean synced(BigInteger key) {
        if (preds == null) // We have no neighbors.
            return true;
        for (GuidRange range : synced_ranges.keySet()) {
            if ((key.compareTo(range.low) >= 0) 
                 && (key.compareTo(range.high) <= 0)) {
                // We're up to date with more than half of our neighbors.
                LinkedHashMap<InetSocketAddress,Long> m = 
                    synced_ranges.get(range);
                if (m.size() > preds.length) {
                    if (logger.isDebugEnabled ())
                        logger.debug("synced with " 
                                     + synced_ranges.get(range).size() 
                                     + " neighbors for key 0x" 
                                     + guid_to_string(key));
                    return true;
                }
                else {
                    String list = "";
                    for (InetSocketAddress n : m.keySet()) {
                        list += n.getAddress().getHostAddress() + ":" 
                              + n.getPort() + " ";
                    }
                    if (logger.isDebugEnabled ())
                        logger.debug("only synced with " + list + "for key 0x" 
                                     + guid_to_string(key));
                    return false;
                }
            }
        }
        if (logger.isDebugEnabled ())
            logger.debug("not responsible for 0x" + guid_to_string(key));
        return false; // We're not responsible for this key.
    }

    protected static String addrToString(InetSocketAddress addr) {
        return addr.getAddress().getHostAddress() + ":" + addr.getPort();
    }
    
    protected void synced(GuidRange range, InetSocketAddress neighbor) {
        LinkedHashMap<InetSocketAddress,Long> s = synced_ranges.get(range);
        if (s == null) {
            s = new LinkedHashMap<InetSocketAddress,Long>();
            synced_ranges.put(range, s);
        }
        assert preds != null;
        // There are at most 2*preds.length distinct neighbors, and at most
        // 2*preds.length ranges we need to synchronize on with each.  We pick
        // them randomly, so we need n log n periods to hit them all.  For 8
        // neighbors and a period of one second, this works out to a timeout
        // of about 6.5 minutes.
        long n = 4*preds.length*preds.length*ae_period;
        long timeout = round(ceil(n * log(n)/log(2)));
        if (logger.isDebugEnabled ())
            logger.debug((s.containsKey(neighbor) ? "re" : "") 
                         + "synced with " + addrToString(neighbor) + " over " 
                         + range + " for " + (timeout/1000) + " seconds");
        s.put(neighbor, new Long(timer_ms() + timeout));
        acore.registerTimer(timeout, curry(syncTimeout, range, neighbor));
    }

    protected Thunk2<GuidRange,InetSocketAddress> syncTimeout =
    new Thunk2<GuidRange,InetSocketAddress>() {
        public void run(GuidRange range, InetSocketAddress neighbor) {
            LinkedHashMap<InetSocketAddress,Long> s = synced_ranges.get(range);
            if (s != null) {
                Long exp = s.get(neighbor);
                if ((exp != null) && timer_ms() >= exp) {
                    if (logger.isDebugEnabled ())
                        logger.debug("sync with " + addrToString(neighbor) 
                                     + " over " + range + " timed out");
                    s.remove(neighbor);
                    if (s.isEmpty())
                        synced_ranges.remove(s);
                }
            }
        }
    };

    protected void unsynced(GuidRange range, InetSocketAddress neighbor) {
        LinkedHashMap<InetSocketAddress,Long> s = synced_ranges.get(range);
        if (s == null) {
            if (logger.isDebugEnabled ())
                logger.debug("still unsynced with " + addrToString(neighbor) 
                             + " over " + range);
        }
        else {
            if (logger.isDebugEnabled ())
                logger.debug("unsynced with " + addrToString(neighbor) 
                             + " over " + range);
            s.remove(neighbor);
            if (s.isEmpty())
                synced_ranges.remove(range);
        }
    }

    protected void handle_ae_alarm (AntiEntropyAlarm alarm) {
        if (ae_state != null) {
            // See if we should time it out.

            if (ae_state.last_activity_ms + 30000 < timer_ms ()) {
                logger.warn("anti-entropy with " + ae_state.ni.node_id 
                            + " timed out");
                unsynced(ae_state.guid_range, ae_state.ni.node_id);
                ae_state = null;
            }
        }

        if (ae_state == null) {

            // Start a new one.

            BambooNeighborInfo ni = random_ls_member ();

            if (ni == null) {
                if (logger.isDebugEnabled ()) logger.debug ("no neighbors");
                classifier.dispatch_later (alarm,
			ae_period + rand.nextInt (ae_period));
                return;
            }

            ae_state = new AntiEntropyState ();
            ae_state.ni = ni;
            ae_state.last_activity_ms = timer_ms ();

            choose_db (ni);

            TreeState ts = tree_state (ae_state.low_guid, ae_state.high_guid);

            // The root is always at the same level and low_time for a given
            // expansion factor.
            ae_state.current_level = ts.tree.root ().level ();
            ae_state.current_low_time = ts.tree.root ().range_low ();

            if (ts.tree.root ().valid (now_ms ())) {
                if (logger.isDebugEnabled ())
                    logger.debug (ae_state + " tree is ready");
                handle_ae_tree_ready (ts.tree);
            }
            else {
                if (logger.isDebugEnabled ()) logger.debug (
			ae_state + " getting tree ready");
                ts.wait_for_ready (new TreeReadyCB () {
                    public void tree_ready (MerkleTree tree) {
                        handle_ae_tree_ready (tree);
                    }
                });
            }
        }

        classifier.dispatch_later (
            new AntiEntropyAlarm (), ae_period + rand.nextInt (ae_period));
    }

    protected void handle_ae_tree_ready (MerkleTree tree) {

        if (ae_state == null) {
            if (logger.isDebugEnabled ()) logger.debug (
                    "ae_state == null, must have timed out");
            return;
        }

        if (logger.isDebugEnabled ()) logger.debug (ae_state + " tree ready");

        MerkleTree.Node node = tree.node (
                ae_state.current_level, ae_state.current_low_time);

        if (node == null) {
            if (logger.isDebugEnabled ())
                logger.debug (ae_state + " tree has changed");
            goto_next_node ();
            return;
        }

        ae_state.fetch_node_seq = next_fetch_seq++;
        ae_state.last_activity_ms = timer_ms ();

        FetchMerkleTreeNodeReq req = new FetchMerkleTreeNodeReq (
                ae_state.ni.node_id, my_guid, ae_state.low_guid,
                ae_state.high_guid, expansion, ae_state.current_level,
                ae_state.current_low_time, node.hash (),
                ae_state.fetch_node_seq);

        dispatch (req);
    }

    protected void handle_fetch_merkle_tree_node_resp (
            final FetchMerkleTreeNodeResp resp) {

        if (ae_state == null) {
            if (logger.isDebugEnabled ()) logger.debug (
                    ae_state + " not expecting " + resp);
            return;
        }

        if (! resp.peer.equals (ae_state.ni.node_id)) {
            if (logger.isDebugEnabled ())
                logger.debug (ae_state + " " + resp +
		    " has bad peer != " + ae_state.ni);
            return;
        }

        if (resp.seq != ae_state.fetch_node_seq) {
            if (logger.isDebugEnabled ()) logger.debug (ae_state + " " + resp
                    + " has bad seq != " + ae_state.fetch_node_seq);
            return;
        }

        ae_state.last_activity_ms = timer_ms ();

        TreeState ts = tree_state (ae_state.low_guid, ae_state.high_guid);

        if (ts.tree.root ().valid (now_ms ())) {
            if (logger.isDebugEnabled ())
                logger.debug (ae_state + " tree is ready");
            handle_ae_tree_ready_resp (ts.tree, resp);
        }
        else {
            if (logger.isDebugEnabled ())
                logger.debug (ae_state + " getting tree ready");
            ts.wait_for_ready (new TreeReadyCB () {
                public FetchMerkleTreeNodeResp r = resp;
                public void tree_ready (MerkleTree tree) {
                    handle_ae_tree_ready_resp (tree, r);
                }
            });
        }
    }

    protected void handle_ae_tree_ready_resp (
            MerkleTree tree, FetchMerkleTreeNodeResp resp) {

        if (ae_state == null) {
            if (logger.isDebugEnabled ()) logger.debug (
                    "ae_state == null, must have timed out");
            return;
        }

        ae_state.last_activity_ms = timer_ms ();

        MerkleTree.Node node = tree.node (
                ae_state.current_level, ae_state.current_low_time);

        if (node == null) {
            if (logger.isDebugEnabled ())
                logger.debug (ae_state + " tree has changed");
            goto_next_node ();
            return;
        }

        if (Arrays.equals (resp.hash, node.hash ())) {
            if (logger.isDebugEnabled ()) logger.debug (ae_state + " match");
            goto_next_node ();
            return;
        }

        if (logger.isDebugEnabled ()) logger.debug (ae_state + " mismatch");

        if ((resp.children == null) || node.children_are_leaves ()) {
            ae_state.fetch_keys_seq = next_fetch_keys_seq++;
            FetchKeysReq req = new FetchKeysReq (ae_state.ni.node_id,
                    ae_state.low_guid, ae_state.high_guid,
                    node.range_low (), node.range_high (),
                    ae_state.fetch_keys_seq);
            dispatch (req);
        }
        else {
            Iterator i = resp.children.iterator ();
            MerkleTree.Node.Iter j = node.children ();
            while (i.hasNext ()) {
                if (! j.hasNext ()) {
                    if (logger.isDebugEnabled ()) logger.debug (
                            ae_state + " mismatch in number of children");
                    goto_next_node ();
                    return;
                }

                byte [] other_hash = (byte []) i.next ();
                MerkleTree.Node child = j.next ();

                if (Arrays.equals (other_hash, child.hash ())) {
                    if (logger.isDebugEnabled ()) logger.debug (
			    ae_state + " match on " + child);
                }
                else {
                    if (logger.isDebugEnabled ()) logger.debug (
			    ae_state + " mismatch on " + child);
                    ae_state.nodes_to_do.addLast (new Pair (
                                new Integer (child.level ()),
                                new Long (child.range_low ())));
                }
            }

            goto_next_node ();
        }
    }

    protected void handle_fetch_merkle_tree_node_reject (
            FetchMerkleTreeNodeReject reject) {
        if (ae_state == null) {
            if (logger.isDebugEnabled ()) logger.debug (
                    ae_state + " unexpected " + reject);
            return;
        }

        ae_state.last_activity_ms = timer_ms ();

        if (reject.reason == FetchMerkleTreeNodeReject.BAD_GUID_RANGE) {
            // Our leaf set was probably in transition.
            if (logger.isDebugEnabled ())
                logger.debug (ae_state + " bad range");
            unsynced(ae_state.guid_range, ae_state.ni.node_id);
            ae_state = null;
        }
        else if (reject.reason == FetchMerkleTreeNodeReject.NO_SUCH_NODE) {
            // Try the next one.
            goto_next_node ();
        }
        else {
            logger.warn("got unexpected reject reason " + reject.reason 
                         + " from " + ae_state.ni);
            unsynced(ae_state.guid_range, ae_state.ni.node_id);
            ae_state = null;
        }
    }

    protected void handle_fetch_keys_resp (FetchKeysResp resp) {
        if (ae_state == null) {
            if (logger.isDebugEnabled ()) logger.debug (
                    "ae_state == null, must have timed out: " + resp);
            return;
        }

        ae_state.last_activity_ms = timer_ms ();

        if (resp.seq != ae_state.fetch_keys_seq) {
            logger.debug (ae_state + " " + resp + " has bad seq != " +
                    ae_state.fetch_node_seq);
            return;
        }
        if (resp.keys == null) {
            // They don't have any data to give us.  We must have mismatched
            // because we have data they don't.  Go on to the next node.

            goto_next_node ();
            return;
        }

        ae_state.fetched_keys.addAll (resp.keys);
        Iterator i = resp.keys.iterator ();
        while (i.hasNext ()) {
            StorageManager.Key k = (StorageManager.Key) i.next ();
            dispatch (new StorageManager.GetByKeyReq (k, my_sink, ae_state));
        }
    }

    protected void handle_fetch_keys_check (StorageManager.GetByKeyResp resp) {
        if (ae_state == null) {
            if (logger.isDebugEnabled ()) logger.debug (
                    "ae_state == null, must have timed out: " + resp);
            return;
        }

        if (ae_state.fetched_keys.remove (resp.key)) {
            if (resp.data == null) {
                // We don't have this data item, so we need to get it.

                ae_state.fetched_data.add (resp.key);
                dispatch (new FetchDataReq (ae_state.ni.node_id, resp.key));
            }
            else {
                // We already have this data item.
            }
        }
        else {
            logger.debug ("unexpected GetByKeyResp");
        }

        if (ae_state.fetched_keys.isEmpty () &&
            ae_state.fetched_data.isEmpty ()) {

            // We're done fetching keys and data.

            goto_next_node ();
        }
    }

    protected void goto_next_node () {

        if (logger.isDebugEnabled ())
            logger.debug (ae_state + " goto_next_node");

        assert ae_state != null;

        TreeState ts = tree_state (ae_state.low_guid, ae_state.high_guid);
        if (ts.tree.root ().valid (now_ms ())) {
            if (logger.isDebugEnabled ())
                logger.debug (ae_state + " tree is ready");
            goto_next_node_ready (ts.tree);
        }
        else {
            if (logger.isDebugEnabled ())
                logger.debug (ae_state + " getting tree ready");
            ts.wait_for_ready (new TreeReadyCB () {
                public void tree_ready (MerkleTree tree) {
                    goto_next_node_ready (tree);
                }
            });
        }
    }

    protected void goto_next_node_ready (MerkleTree tree) {

        if (logger.isDebugEnabled ())
            logger.debug (ae_state + " goto_next_node_ready");

        if (ae_state == null) {
            if (logger.isDebugEnabled ()) logger.debug (
                    "ae_state == null, must have timed out");
            return;
        }

        MerkleTree.Node node = null;
        while (! ae_state.nodes_to_do.isEmpty ()) {

            Pair p = (Pair) ae_state.nodes_to_do.removeFirst ();
            int level = ((Integer) p.first).intValue ();
            long low_time = ((Long) p.second).longValue ();
            node = tree.node (level, low_time);
            if (node == null) {
                if (logger.isDebugEnabled ()) logger.debug (
                        ae_state + " no longer a node");
            }
            else {
                break;
            }
        }

        if (node == null) {
            if (logger.isDebugEnabled ()) logger.debug (ae_state +
                    " all nodes finished--ae done");
            synced(ae_state.guid_range, ae_state.ni.node_id);
            ae_state = null;
            return;
        }

        ae_state.current_level = node.level ();
        ae_state.current_low_time = node.range_low ();
        handle_ae_tree_ready (tree);
    }

    protected void handle_get_by_key_resp (StorageManager.GetByKeyResp resp) {
        if (resp.user_data instanceof AntiEntropyState) {
            if (resp.user_data == ae_state)
                handle_fetch_keys_check (resp);
            else
                ; // must have timed out so just drop it (no cursor to close)
        }
        else if (resp.user_data instanceof FetchDataReq) {
            handle_fetch_data_req_have_data (resp);
        }
        else {
            assert false : resp.user_data.getClass ().getName ();
        }
    }

    protected void handle_fetch_data_resp (FetchDataResp resp) {
        if (ae_state == null) {
            if (logger.isDebugEnabled ()) logger.debug (
		    ae_state + " unexpected " + resp);
            return;
        }
        if (resp.data == null) {
            if (logger.isDebugEnabled ()) logger.debug (
		    ae_state + " no data in " + resp);
            return;
        }

        if (ae_state.fetched_data.remove (resp.key) &&
	    ((resp.key.expiryTime()) > ((long) now_ms() * 1000))) {

            boolean valid = true;
            if (!resp.key.put) {
                byte [] secretHash = md.digest(resp.data.array());
                if (!Arrays.equals(resp.key.secret_hash, secretHash)) {
                    StringBuffer buf = new StringBuffer (200);
                    buf.append ("got bad remove ");
                    resp.key.toStringBuffer(buf);
                    buf.append (" hash(data)=0x");
                    bytes_to_sbuf(secretHash, 0, 4, buf);
                    buf.append (" from neighbor ");
                    addr_to_sbuf(resp.peer, buf);
                    logger.warn(buf);
                    valid = false;
                }
            }

            if (valid) {
                if (logger.isInfoEnabled ()) {
                    StringBuffer buf = new StringBuffer (200);
                    buf.append ("got ");
                    resp.key.toStringBuffer(buf);
                    buf.append (" size=");
                    buf.append (resp.data.limit ());
                    buf.append (" from neighbor ");
                    addr_to_sbuf(resp.peer, buf);
                    logger.info (buf);
                }
                unsynced(ae_state.guid_range, ae_state.ni.node_id);
                db_put(resp.key, resp.data, null);
            }
        }
        else {
            if (logger.isDebugEnabled ()) logger.debug (
		    ae_state + " unrecognized key");
            return;
        }

        if (ae_state.fetched_keys.isEmpty () &&
            ae_state.fetched_data.isEmpty ()) {
            goto_next_node ();
        }
    }

    //////////////////// SERVER-SIDE ANTI_ENTROPY STATE /////////////////////

    protected boolean valid_db (NodeId peer, BigInteger peer_guid,
            BigInteger low, BigInteger high) {

	if (preds == null)
	    return false;
        BambooNeighborInfo other = new BambooNeighborInfo (peer, peer_guid, 0.0);
        GuidRange range = new GuidRange (low, high);
        SortedSet choices = calc_shared_dbs (other);
        if (choices != null) {
            logger.debug("valid range");
            return choices.contains (range);
        }
        else {
            logger.debug("invalid range");
            return false;
        }
    }

    protected void handle_fetch_merkle_tree_node_req (
            final FetchMerkleTreeNodeReq req) {

        if (! valid_db (req.peer, req.peer_guid, req.low_guid, req.high_guid)) {
            dispatch (new FetchMerkleTreeNodeReject (req.peer,
                        FetchMerkleTreeNodeReject.BAD_GUID_RANGE, req.seq));
            return;
        }

        if (req.expansion != expansion) {
            dispatch (new FetchMerkleTreeNodeReject (req.peer,
                        FetchMerkleTreeNodeReject.BAD_EXPANSION, req.seq));
            return;
        }

        TreeState ts = tree_state (req.low_guid, req.high_guid);

        if (logger.isDebugEnabled ()) logger.debug ("db range is " +
                (new GuidRange (req.low_guid, req.high_guid)).toString ());

        ts.wait_for_ready (new TreeReadyCB () {
            public FetchMerkleTreeNodeReq r = req;
            public void tree_ready (MerkleTree tree) {
                handle_fetch_tree_ready (tree, r);
            }
        });
    }

    protected void handle_fetch_tree_ready (
            MerkleTree tree, FetchMerkleTreeNodeReq req) {

        MerkleTree.Node node = tree.node (req.level, req.low_time);
        if (node == null) {
            dispatch (new FetchMerkleTreeNodeReject (req.peer,
                        FetchMerkleTreeNodeReject.NO_SUCH_NODE, req.seq));
            return;
        }

        boolean leaf = node.children_are_leaves ();
        LinkedList children = null;

        if ((! leaf) && (!Arrays.equals (node.hash (), req.expected_hash))) {
            children = new LinkedList ();
            MerkleTree.Node.Iter i = node.children ();
            while (i.hasNext ()) {
                MerkleTree.Node child = i.next ();
                children.addLast (child.hash ());
            }
        }

        FetchMerkleTreeNodeResp resp = new FetchMerkleTreeNodeResp (
                req.peer, node.hash (), leaf, children, req.seq);
        dispatch (resp);
    }

    protected void handle_fetch_keys_req (FetchKeysReq req) {
        Pair ud = new Pair (req, null);
        StorageManager.GetByTimeReq outb =
            new StorageManager.GetByTimeReq (
                    req.low_time, req.high_time, my_sink, ud);
        dispatch (outb);
    }

    protected void handle_fetch_keys_get_resp (
            StorageManager.GetByTimeResp resp) {

        Pair ud = (Pair) resp.user_data;
        FetchKeysReq req = (FetchKeysReq) ud.first;
        LinkedList keys = (LinkedList) ud.second;

	// Protect against too large response messages.  The client may
	// have asked for too large a range.  As long as we give it back
	// more than enough to form a full node, it will split its existing
	// node and ask for a smaller range next time.

        if (resp.continuation != null) {
            if (keys == null) ud.second = keys = new LinkedList ();
            for (StorageManager.Key k : resp.keys) {
                if (keys.size () >= (1<<expansion) + 1)
                    break;
                if (in_range_mod (req.low_guid, req.high_guid, k.guid))
                    keys.addLast (k);
            }
        }

        if ((resp.continuation == null) ||
                ((keys != null) && (keys.size () >= (1<<expansion) + 1))) {

            // Close the cursor.
            
            if (resp.continuation != null)
                dispatch (new StorageManager.GetByTimeCont (
                            resp.continuation, null, null));

            // Send the response.

            dispatch (new FetchKeysResp (req.peer, keys, req.seq));
        }
        else {

            // Get more keys.

            dispatch (new StorageManager.GetByTimeCont (
                        resp.continuation, my_sink, ud));
        }
    }

    protected void handle_fetch_data_req (FetchDataReq req) {
        dispatch (new StorageManager.GetByKeyReq (req.key, my_sink, req));
    }

    protected void handle_fetch_data_req_have_data (
            StorageManager.GetByKeyResp resp) {
        FetchDataReq req = (FetchDataReq) resp.user_data;
        dispatch (new FetchDataResp (req.peer, req.key, resp.data));
    }

    /////////////////////////////////////////////////////////////////////////
    //
    // Replica set request and response code.  This code is used to find a
    // good node to discard data onto.
    //
    /////////////////////////////////////////////////////////////////////////

    protected BambooNeighborInfo my_neighbor_info;

    protected void handle_replica_set_req (ReplicaSetReq req) {
	NodeId [] result = null;
	if (preds == null) {
	    NodeId [] x = {my_node_id};
	    result = x;
	}
	else {

	    int unique_count = 0;
	    {
		TreeSet s = new TreeSet ();
		for (int i = 0; i < preds.length; ++i)
		    s.add (preds [i]);
		for (int i = 0; i < succs.length; ++i)
		    s.add (succs [i]);
		unique_count = s.size ();
	    }

	    int pred_max = 0, succ_max = 0;
	    if (unique_count == preds.length + succs.length) {
		// If we have a full leaf set, return all but one of them.
		// If the guid precedes us, eliminate one of our successors
		// and vice versa.

		result = new NodeId [unique_count];
		pred_max = preds.length - 1;
		if (! in_range_mod (preds [0].guid, my_guid, req.guid))
		    --pred_max;
		succ_max = succs.length - 1;
		if (! in_range_mod (my_guid, succs [0].guid, req.guid))
		    --succ_max;
	    }
	    else {
		// If we don't have a full leaf set, only return the unique
		// nodes.  Take half from the predecessors and half from
		// the successors.

		result = new NodeId [unique_count + 1];
		pred_max = (unique_count - 1) / 2;
	    }

	    int j = 0;

	    for (int i = pred_max; i >= 0; --i) {
		if (logger.isDebugEnabled ()) logger.debug (
			"j=" + j + " adding " + preds [i]);
		result [j++] = preds [i].node_id;
	    }
	    if (logger.isDebugEnabled ()) logger.debug (
		    "j=" + j + " adding " + my_neighbor_info);
	    result [j++] = my_node_id;

	    for (int i = 0; j < result.length; ++i) {
		if (logger.isDebugEnabled ()) logger.debug (
			"j=" + j + " adding " + succs [i]);
		result [j++] = succs [i].node_id;
	    }
	}

	dispatch (new ReplicaSetResp (req.return_address, req.nonce, result));
    }

    /////////////////////// CLIENT-SIDE DISCARD STATE /////////////////////////
    //
    // This state machine periodically goes through the database of recycled
    // tuples (those whose keys are not within our leaf set), removes each
    // tuple it finds there, and sends it to a node whose leaf set does
    // contain its key.
    //
    ///////////////////////////////////////////////////////////////////////////

    protected static class DiscardAlarm implements QueueElementIF {
        public boolean periodic;
        public DiscardAlarm (boolean p) { periodic = p; }
    }

    protected static class DiscardState {
        public boolean wrapped;
        public StorageManager.Key key;
        public ByteBuffer data;
        public long nonce;
        public long timeout_ms = -1;
        public Object cont;
    }

    protected DiscardState ds;

    protected void handle_discard_alarm (DiscardAlarm item) {

        if (ds != null) {
            if ((ds.timeout_ms != -1) && (timer_ms () > ds.timeout_ms)) {
                if (logger.isInfoEnabled ()) {
                    StringBuffer buf = new StringBuffer (200);
                    buf.append ("discard timeout on ");
                    ds.key.toStringBuffer(buf);
                    logger.info (buf);
                }

                dispatch (new StorageManager.GetByGuidCont (
                            ds.cont, false /* don't delete */, null, null));
                ds = null;
            }
        }

        if (ds == null) {
            // Start with a random guid.

            ds = new DiscardState ();
            ds.nonce = next_put_or_remove_seq++;
            dispatch (new StorageManager.GetByGuidReq (
                        GuidTools.random_guid (rand), false /* recycling */,
                        null, my_sink, ds));
        }

        if (item.periodic)
            classifier.dispatch_later (item, 5000 + rand.nextInt (5000));
    }

    protected void handle_discard_get_resp (StorageManager.GetByGuidResp resp) {

        if (resp.continuation == null) {
            // There were no pointers between the search guid and MAX_GUID.

            if (ds.wrapped) {
                // If we've already wrapped around, there's nothing to
                // discard.  Try again later.

                ds = null;
            }
            else {
                // If this was the first search, try wrapping around.

                ds.wrapped = true;
                dispatch (new StorageManager.GetByGuidReq (
                            MIN_GUID, false, null, my_sink, ds));
            }
        }
        else {
            ds.key = resp.key;
            ds.data = resp.data;
            ds.cont = resp.continuation;
            ds.timeout_ms = timer_ms () + 60*1000;

            // Find a new node to send it to.

            dispatch (new BambooRouteInit (ds.key.guid, app_id,
                        false /* no upcalls */, iterative_routing,
                        new ReplicaSetReq (my_node_id, ds.key.guid, ds.nonce)));
        }
    }

    protected void handle_replica_set_resp (ReplicaSetResp resp) {
        if ((ds == null) || (resp.nonce != ds.nonce)) {
            if (logger.isDebugEnabled ()) logger.debug ("unexpected " + resp);
            return;
        }

        int which = rand.nextInt (resp.replica_set.length);

        // Send the tuple to a random member of the replica set.

        if (logger.isInfoEnabled ()) {
            StringBuffer buf = new StringBuffer (200);
            buf.append ("discarding ");
            ds.key.toStringBuffer(buf);
            buf.append (" to ");
            addr_to_sbuf(resp.replica_set [which], buf);
            logger.info (buf);
        }

        PutOrRemoveMsg outb = new PutOrRemoveMsg (
                resp.replica_set [which], ds.key.time_usec, ds.key.ttl_sec,
		ds.key.guid, ds.data, ds.key.put, ds.key.client_id,
                ds.nonce, ds.key.data_hash, ds.key.secret_hash);

        dispatch (outb);
    }
}
