/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.openhash.redir;

import bamboo.dht.GatewayClient;
import bamboo.util.GuidTools;
import bamboo.util.StandardStage;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.Set;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import bamboo.lss.ASyncCore;

// These are the RPC datatypes produced by compiling the .x file:
import bamboo.dht.bamboo_put_args;
import bamboo.dht.bamboo_get_args;
import bamboo.dht.bamboo_get_res;
import bamboo.dht.bamboo_key;
import bamboo.dht.bamboo_placemark;
import bamboo.dht.bamboo_value;
import bamboo.dht.bamboo_stat;

/**
 * An implementation of ReDiR.
 *
 * @author Sean C. Rhea
 * @version $Id: RedirClient.java,v 1.11 2004/11/19 23:20:14 srhea Exp $
 */
public class RedirClient extends StandardStage
implements SingleThreadedEventHandlerIF {
    protected static BigInteger TWO = BigInteger.valueOf(2);
    protected static BigInteger MOD = (BigInteger.valueOf(2)).pow(160);

    protected static BigInteger rendevous_point(
        BigInteger key, BigInteger namespace, int level) {
        BigInteger two2thel = TWO.pow(level);
        BigInteger partition_width = MOD.divide(two2thel);
        /*if (logger.isDebugEnabled ())
            logger.debug ("two2thel=0x" + two2thel.toString (16));*/
        /*if (logger.isDebugEnabled ())
            logger.debug ("partition_width=0x"
                    + partition_width.toString (16));*/
        // use the key to find the right partition
        BigInteger partition_number = key.divide(partition_width);
        BigInteger low = partition_number.multiply(partition_width);
        /*if (logger.isDebugEnabled ())
            logger.debug ("low=0x" + low.toString (16));*/
        // then use the namespace to find the right point in that partition
        // From the IPTPS paper, k=P+|_k(P-Q)/K_|
        //                        =low+namespace*(partition_width)/MOD
        //                        =low+namespace*(MOD/two2thel)/MOD
        //                        =low+namespace/two2thel
        return low.add(namespace.divide(two2thel));
    }

    public static InetSocketAddress bytes2addr(byte[] bytes) {
        byte[] addrb = new byte[4];
        System.arraycopy(bytes, 0, addrb, 0, 4);
        InetAddress addr = null;
        try { addr = InetAddress.getByAddress(addrb); }
        catch (UnknownHostException e) { assert false:e; }
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.position(4);
        int port = bb.getShort();
	if (port < 0) port += 65536;
        return new InetSocketAddress(addr, port);
    }

    public static byte[] addr2bytes(InetSocketAddress addr) {
        byte[] result = new byte[6];
        System.arraycopy(addr.getAddress().getAddress(), 0, result, 0, 4);
        ByteBuffer bb = ByteBuffer.wrap(result);
        bb.position(4);
        bb.putShort( (short) addr.getPort());
        return result;
    }

    public static byte[] bi2bytes(BigInteger i) {
        byte[] result = i.toByteArray();
        if (result.length == 20)
            return result;
        byte[] rightsize = new byte[20];
        if (result.length > 20)
            System.arraycopy(result, result.length - 20, rightsize, 0, 20);
        else
            System.arraycopy(result, 0, rightsize, 20 - result.length,
                             result.length);
        return rightsize;
    }

    public static BigInteger bytes2bi(byte[] bytes) {
        // ensure positive
        if ( (bytes[0] & 0x80) != 0) {
            byte[] copy = new byte[bytes.length + 1];
            System.arraycopy(bytes, 0, copy, 1, bytes.length);
            bytes = copy;
        }
        return new BigInteger(bytes);
    }

    protected GatewayClient client;
    protected MessageDigest digest;
    protected Set namespaces = new HashSet();

    public void init(ConfigDataIF config) throws Exception {
        super.init(config);
        String client_stg_name =
            config_get_string(config, "client_stage_name");
        client = (GatewayClient) lookup_stage(config, client_stg_name);
        digest = MessageDigest.getInstance("SHA");
    }

    public interface JoinCb { void join_cb(Object user_data); }

    public static class JoinState {
        public InetSocketAddress addr;
        public byte [] addr_bytes;
        public BigInteger key;
        public BigInteger namespace;
        public int levels;
        public int ttl_s;
        public JoinCb cb;
        public Object user_data;
        public int current_level;
        public boolean done;
        public boolean found;
        public String application;
        public long put_retry_time = 1000;
	public Object retry_timer=null, rejoin_timer=null;
	public boolean cancelled=false;

        public JoinState(InetSocketAddress a, BigInteger k, BigInteger n,
                         int l, int t, String app, JoinCb c, Object u) {
            addr = a; key = k; namespace = n; levels = l; ttl_s = t; cb = c;
            user_data = u; current_level = levels - 1; done = false;
            addr_bytes = addr2bytes (addr); application = app;
        }
    }

    public Object join(InetSocketAddress addr, BigInteger key,
		       BigInteger namespace, int levels,
		       int ttl_s, String application, JoinCb cb, 
                       Object user_data) {
        if (logger.isInfoEnabled()) {
            logger.info("joining addr=" + addr.getAddress ().getHostAddress () 
                        + ":" + addr.getPort () + " key=0x" 
                        + GuidTools.guid_to_string(key) + " namespace=0x"
                        + GuidTools.guid_to_string(namespace) + " levels="
                        + levels + " ttl_s=" + ttl_s);
        }
        JoinState state = new JoinState(addr, key, namespace, levels, ttl_s, 
                                        application, cb, user_data);
        join_get(state);
	return state;
    }

    public void cancel_join(Object join_token) {
	JoinState state = (JoinState) join_token;
	if (state.cancelled) return;
	if (state.retry_timer != null) {
	    acore.cancel_timer(state.retry_timer);
	    state.retry_timer = null;
	}
	if (state.rejoin_timer != null) {
	    acore.cancel_timer(state.rejoin_timer);
	    state.rejoin_timer = null;
	}
	state.cancelled = true;
    }


    protected void join_get(JoinState state) {
        BigInteger key = rendevous_point(
            state.key, state.namespace, state.current_level);

        if (logger.isDebugEnabled())
            logger.debug("namespace=0x"
                         + GuidTools.guid_to_string(state.namespace)
                         + " current_level=" + state.current_level
                         + " rendevous_point=0x"
                         + GuidTools.guid_to_string(key));

        bamboo_get_args get_args = new bamboo_get_args();
        get_args.application = state.application;
        get_args.key = new bamboo_key();
        get_args.key.value = bi2bytes(key);
        get_args.maxvals = Integer.MAX_VALUE;
        get_args.placemark = new bamboo_placemark();
        get_args.placemark.value = new byte[] {};

        state.found = false;
        Object[] pair = { state, get_args};
        if (logger.isDebugEnabled())
            logger.debug("doing join get 0x" + GuidTools.guid_to_string(key));
        client.get(get_args, join_get_done_cb, pair);
    }

    public GatewayClient.GetDoneCb join_get_done_cb = new GatewayClient.
        GetDoneCb() {
        public void get_done_cb(bamboo_get_res get_res, Object user_data) {
            Object[] pair = (Object[]) user_data;
            JoinState state = (JoinState) pair[0];
            if (get_res.values.length == 0) {
                if (logger.isDebugEnabled()) 
                    logger.debug(state.found ? "no more values" : "no values");
                join_put(state);
                return;
            }

            state.found = true;
            for (int i = 0; i < get_res.values.length; ++i) {
                BigInteger hash =
                    bytes2bi(digest.digest(get_res.values[i].value));
                if (hash.compareTo(state.key) < 0) {
                    // we found a predecessor
                    if (logger.isDebugEnabled()) {
                        logger.debug("found predecessor 0x" +
                                     GuidTools.guid_to_string(hash));
                    }
                    state.done = true;
                    join_put(state);
                    return;
                }
            }

            if (get_res.placemark.value.length == 0) {
                // that's all the values there are
                if (logger.isDebugEnabled ())
                    logger.debug ("empty placemark--no more values");
                join_put(state);
            }
            else {
                // we found no predecessors, but there may be more values
                if (logger.isDebugEnabled())
                    logger.debug("no predecessor yet: pl.len=" +
                            get_res.placemark.value.length);
                bamboo_get_args get_args = (bamboo_get_args) pair[1];
                get_args.placemark = get_res.placemark;
                client.get(get_args, this, pair);
            }
        }
    };

    protected void join_put(JoinState state) {
        BigInteger key = rendevous_point(
            state.key, state.namespace, state.current_level);

        bamboo_put_args put = new bamboo_put_args();
        put.application = state.application;
        put.value = new bamboo_value();
        put.value.value = state.addr_bytes;
        put.key = new bamboo_key();
        put.key.value = bi2bytes(key);
        put.ttl_sec = (state.ttl_s);

        if (logger.isDebugEnabled())
            logger.debug("doing join put 0x" + GuidTools.guid_to_string(key));

        client.put(put, join_put_done_cb, state);
    }

    public GatewayClient.PutDoneCb join_put_done_cb = new GatewayClient.
        PutDoneCb() {
        public void put_done_cb(int put_res, Object user_data) {
            JoinState state = (JoinState) user_data;
            if (put_res == bamboo_stat.BAMBOO_OK) {
                state.put_retry_time = 1000;
                if (state.done || (state.current_level == 0)) {
                    if (logger.isInfoEnabled()) {
                        logger.info("successfully joined namespace=0x"
                                    + GuidTools.guid_to_string(state.namespace)
                                    + " levels=" + state.levels
                                    + " ttl_s=" + state.ttl_s);
                    }
                    if (!state.cancelled) state.cb.join_cb(state.user_data);

		    /* the app may cancel the join in the callback,
		       so check the cancelled flag AGAIN here */
		    if (!state.cancelled) {
			state.current_level = state.levels - 1;
			state.done = false;
			state.rejoin_timer = acore.register_timer(
                                state.ttl_s*1000/2, rejoin_cb, state);
		    }
                }
                else {
                    --state.current_level;
                    join_get(state);
                }
            }
            else {
                // Try again.
		if (!state.cancelled) {
		    logger.info("join put failed, trying again");
		    state.put_retry_time = Math.min(state.put_retry_time * 2,
						    30 * 1000);
		    state.retry_timer = acore.register_timer(state.put_retry_time, put_retry_cb, state);
		}
            }
        }
    };

    public ASyncCore.TimerCB put_retry_cb = new ASyncCore.TimerCB() {
        public void timer_cb(Object user_data) {
	    ((JoinState) user_data).retry_timer = null;
            join_put( (JoinState) user_data);
        }
    };

    public ASyncCore.TimerCB rejoin_cb = new ASyncCore.TimerCB () {
        public void timer_cb(Object user_data) {
            JoinState state = (JoinState) user_data;
	    state.rejoin_timer = null;
	    if (state.cancelled) return;

            if (logger.isInfoEnabled()) {
                logger.info("rejoining namespace=0x"
                            + GuidTools.guid_to_string(state.namespace) +
                            " levels=" + state.levels + " ttl_s=" +
                            state.ttl_s);
            }
            join_get (state);
        }
    };

    public interface LookupCb {
        void lookup_cb (BigInteger key, InetSocketAddress succ_addr,
                BigInteger succ_hash, int gets, Object user_data);
    }

    public static class LookupState {
        public BigInteger key;
        public BigInteger namespace;
        public int levels;
        public LookupCb cb;
        public Object user_data;
        public int current_level;
        public BigInteger min_hash;
        public InetSocketAddress min_addr;
        public InetSocketAddress succ_addr;
        public BigInteger succ_hash;
        public int gets;
        public String application;
        public LookupState (BigInteger k, BigInteger n, int l, String app, 
                            LookupCb c, Object u) {
            key = k; namespace = n; levels = l; cb = c; user_data = u;
            current_level = levels - 1; application = app;
        }
    }

    public void lookup (BigInteger key, BigInteger namespace, int levels,
            String application, LookupCb cb, Object user_data) {
        if (logger.isDebugEnabled ()) {
            logger.debug ("looking up key=0x"
                    + GuidTools.guid_to_string (key)
                    + " namespace=0x" + GuidTools.guid_to_string (namespace)
                    + " levels=" + levels);
        }
        LookupState state = new LookupState (key, namespace, levels, 
                                             application, cb, user_data);
        lookup_get (state);
    }

    protected void lookup_get (LookupState state) {
        BigInteger key = rendevous_point (
                state.key, state.namespace, state.current_level);

        if (logger.isDebugEnabled ())
            logger.debug ("namespace=0x"
                    + GuidTools.guid_to_string (state.namespace)
                    + " current_level=" + state.current_level
                    + " rendevous_point=0x"
                    + GuidTools.guid_to_string (key));

        bamboo_get_args get_args = new bamboo_get_args ();
        get_args.application = state.application;
        get_args.key = new bamboo_key ();
        get_args.key.value = bi2bytes (key);
        get_args.maxvals = Integer.MAX_VALUE;
        get_args.placemark = new bamboo_placemark ();
        get_args.placemark.value = new byte [] {};

        Object [] pair = {state, get_args};
        if (logger.isDebugEnabled ())
            logger.debug ("doing lookup get 0x"
                    + GuidTools.guid_to_string (key));
        client.get (get_args, lookup_done_cb, pair);
        ++state.gets;
    }

    public GatewayClient.GetDoneCb lookup_done_cb = new GatewayClient.GetDoneCb () {
        public void get_done_cb (bamboo_get_res get_res, Object user_data) {
            Object [] pair = (Object []) user_data;
            LookupState state = (LookupState) pair [0];
            if (get_res.values.length == 0) {
                if (state.succ_hash == null) {
                    if (logger.isDebugEnabled ())
                        logger.debug ("no values");
                    lookup_no_successor (state);
                }
                else {
                    if (logger.isDebugEnabled ())
                        logger.debug ("no more values");
                    lookup_result (state, state.succ_addr, state.succ_hash);
                }
                return;
            }

            for (int i = 0; i < get_res.values.length; ++i) {
                BigInteger hash =
                    bytes2bi (digest.digest (get_res.values [i].value));
                InetSocketAddress addr = bytes2addr (get_res.values [i].value);
                if (hash.compareTo (state.key) > 0) {
                    // we found a successor
                    if (logger.isDebugEnabled ()) {
                        logger.debug ("found successor 0x"
                                + GuidTools.guid_to_string (hash)
                                + ", " + addr.getAddress ().getHostAddress ()
                                + ":" + addr.getPort ());
                    }
                    if ((state.succ_hash == null)
                        || (hash.compareTo (state.succ_hash) < 0)) {
                        state.succ_hash = hash;
                        state.succ_addr = addr;
                    }
                }
                if ((state.min_hash == null)
                    || (hash.compareTo (state.min_hash) < 0)) {
                    state.min_hash = hash;
                    state.min_addr = addr;
                }
            }

            if (get_res.placemark.value.length == 0) {
                // that's all the values there are
                if (logger.isDebugEnabled ())
                    logger.debug ("empty placemark--no more values");
                if (state.succ_hash == null) 
                    lookup_no_successor (state);
                else 
                    lookup_result (state, state.succ_addr, state.succ_hash);
            }
            else {
                // there may be a better successor still
                if (logger.isDebugEnabled ())
                    logger.debug ("looking for more successors");
                bamboo_get_args get_args = (bamboo_get_args) pair [1];
                get_args.placemark = get_res.placemark;
                client.get (get_args, this, pair);
            }
        }
    };

    protected void lookup_no_successor (LookupState state) {
        if (state.current_level == 0) {
            if (logger.isDebugEnabled ()) {
                if (state.min_addr == null)
                    logger.debug ("namespace has no members");
                else
                    logger.debug ("wrapped around");
            }
            lookup_result (state, state.min_addr, state.min_hash);
        }
        else {
            --state.current_level;
            lookup_get (state);
        }
    }

    protected void lookup_result (LookupState state,
            InetSocketAddress succ_addr, BigInteger succ_hash) {
        if (logger.isDebugEnabled ()) {
            if (succ_addr == null)
                logger.debug ("lookup returning null");
            else
                logger.debug ("lookup returning 0x"
                        + GuidTools.guid_to_string (succ_hash)
                        + ", " + succ_addr.getAddress ().getHostAddress ()
                        + ":" + succ_addr.getPort ());
        }
        state.cb.lookup_cb (state.key, succ_addr, succ_hash, state.gets,
                state.user_data);
    }

    public void handleEvent (QueueElementIF item) {
        BUG ("unexpected event: " + item);
    }
}

