/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.lss;
import bamboo.util.Pair;
import bamboo.util.StringUtil;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.security.Key;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map;
import java.util.Set;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import static bamboo.util.Curry.*;

/**
 * A TCP-friendly datagram layer.
 *
 * @author Sean C. Rhea
 * @version $Id: UdpCC.java,v 1.54 2005/08/15 22:22:29 srhea Exp $
 */
public class UdpCC {

    protected boolean DEBUG_MIN = false;
    protected boolean DEBUG_RTT = false;
    protected boolean DEBUG = false;

    public void track_duplicates (long period) {
        if ((track_duplicates_period == 0) && (period != 0)) {
            acore.register_timer (period, dup_track_cb, null);
        }
        track_duplicates_period = period;
    }

    protected long track_duplicates_period, received_msgs, received_duplicates;

    protected ASyncCore.TimerCB dup_track_cb = new ASyncCore.TimerCB () {
        public void timer_cb (Object not_used) {
            logger.info ("recv=" + received_msgs + " dup=" +
                    received_duplicates + " (" + ((received_msgs > 0) ? 
                    ((double) received_duplicates / received_msgs * 100.0) :
                    0.0) + "%)");
            received_msgs = 0; received_duplicates = 0;
            if (track_duplicates_period != 0) {
                acore.register_timer (track_duplicates_period, 
                                      dup_track_cb, null);
            }
        }
    };

    protected int SOCKBUF_MAX = 1048575;

    protected Logger logger;

    public static interface Serializer {
        /**
         * Called to determine the size of the byte array needed to
         * serialize the given <code>msg</code> into.
         */
	int serialize_size (Object msg);

        /**
         * Called to serialize the given <code>msg</code> into the provided
         * <code>buf</code>; the inverse of <code>deserialize</code>.
         */
	void serialize (Object msg, ByteBuffer buf);

        /**
         * Called to deserialize the given <code>buf</code> into the message
         * it represents; the inverse of <code>serialize</code>.
         */
	Object deserialize (ByteBuffer buf) throws Exception;
    }

    public static interface Sink {
        /**
         * Called when a message is received.
         *
         * @param msg        the message that was received
         *
         * @param src        the host and port from which the message was
         *                   sent.  The source is not authenticated; this
         *                   value is just what is read out of the IP packet
         *                   header
         *
         * @param local      the localhost and port on which the message was
         *                   received
         *
         * @param tries      the number of times the message has been sent,
         *                   including this one, or -1 if that information is
         *                   not available (for example, if the message was
         *                   sent with {@link #send_nocc})
         *
         * @param wait_ms    the time in milliseconds that the message sat in
         *                   the sender's outbound queue before being sent,
         *                   presumably because it was waiting behind other
         *                   messages, or -1 if that information is not
         *                   available
         *
         * @param est_rtt_ms the sender's estimate of the round trip time in
         *                   milliseconds between it and this host, or -1 if
         *                   that information is not available
         */
	void recv (Object msg, InetSocketAddress src, InetSocketAddress local,
                   int tries, long wait_ms, long est_rtt_ms);
    }

    public static interface SendCB {
        /**
         * The callback indicating the UdpCC layer is done with a particular
         * message.
         *
         * @param user_data     the data supplied to <code>send</code>
         * @param success       whether the message was acknowledged by the
         *                      recipient
         */
	void cb (Object user_data, boolean success);
    }

    /**
     * Construct a new UdpCC object with a UDP socket bound to
     * <code>address<code> and start listening for messages.
     */
    public UdpCC (ASyncCore core, InetSocketAddress address,
	    Serializer slz, Sink snk) {

        logger = Logger.getLogger (getClass ());

	acore = core;
	my_addr = address;
	serializer = slz;
	sink = snk;
        next_msg_id = System.currentTimeMillis () >> 8;
        if (next_msg_id < 0)
            BUG ("next_msg_id=" + next_msg_id);

	try {
	    channel = DatagramChannel.open ();
	    sock = channel.socket ();
	    int rxsize = sock.getReceiveBufferSize();
	    logger.info("initial rcv sobuf = " + rxsize);
	    sock.bind (my_addr);
	    channel.configureBlocking (false);
	    skey = acore.register_selectable (channel, SelectionKey.OP_READ,
		    new MySelectableCB (), null);
	}
	catch (IOException e) {
	    logger.fatal ("could not open " + my_addr, e);
	    System.exit (1);
	}

	// acore.register_timer (STATS_PERIOD, new StatsCB (), null);
        acore.register_timer (BW_STATS_PERIOD, new BandwidthCB (), null);
        acore.registerTimer(10*1000, printBytes);
    }

    /**
     * Closes the socket associated with this object, removes its callbacks
     * from ASyncCore, and causes it to stop responding to any outstanding
     * timers it has registered--in other words, turns it off.  There is no
     * way to turn it back on; instead, just create a new one.
     */
    public void close () {
        acore.unregister_selectable (skey);
        sock.close (); // closes channel with it
        closed = true;
    }

    public int debug_level () {
	return ((DEBUG)     ? 0x4 : 0x0) |
               ((DEBUG_RTT) ? 0x2 : 0x0) |
               ((DEBUG_MIN) ? 0x1 : 0x0);
    }

    public void set_debug_level (int value) {
	DEBUG_MIN = ((value & 0x1) != 0);
	DEBUG_RTT = ((value & 0x2) != 0);
	DEBUG     = ((value & 0x4) != 0);
        if (value > 0) logger.setLevel (Level.DEBUG);
    }

    public void set_sockbuf_size (int value) {
	if ((value > 0) && (value <= SOCKBUF_MAX)) {
	    try {
		sock.setReceiveBufferSize(value);
		int rxsize = sock.getReceiveBufferSize();
		logger.info("set rcv sobuf " + value + "; got " + rxsize);
	    }
	    catch (SocketException e) {
		logger.fatal ("could not set socket buffer size " + value, e);
		System.exit (1);
	    }
	}
    }
	    
    public void set_timeout_factor (double value) {
        timeout_factor = value;
        logger.info ("timeout_factor=" + timeout_factor);
    }

    public void set_timeout_diff (double value) {
        timeout_diff = value;
        logger.info ("timeout_diff=" + timeout_diff);
    }

    protected Mac mac;
    public void set_mac_key (String keyfile) throws Exception {

        // There is no way to compute a MAC over a direct buffer except by
        // copying it to a byte [], which defeats the whole purpose of having
        // a direct buffer in the first place.

        USE_DIRECT = false;
        reuse_buf = ByteBuffer.allocate (MAX_MSG_SIZE);

        FileInputStream file = new FileInputStream (keyfile);
        byte [] keymat = new byte [20];
        file.read (keymat, 0, 20);
        
        mac = Mac.getInstance("HmacSHA1");
        Key key = new SecretKeySpec(keymat, 0, keymat.length, "HmacSHA1");
        mac.init(key);
    }

    /**
     * This is just a one-shot, minimal-delay send without an acknowledgement;
     * <i>Do not use this function unless you are providing some
     * application-level congestion control</i>.  Messages send with this
     * function go into their own per-destination queue, separate from
     * messages sent with {@link
     * #send(Object,InetSocketAddress,long,Thunk1<Boolean>)}
     */
    public void send_nocc (Object msg, InetSocketAddress dst) {

	long now_ms = System.currentTimeMillis ();

	Connection conn = (Connection) conns.get (dst);
	if (conn == null) {
	    conn = new Connection (dst, now_ms);
	    conns.put (dst, conn);
	}

	conn.probe_q.addLast (msg);

	if (conn.writable ())
	    add_to_rr (conn);
    }

    /**
     * Send a congestion-controlled message to another host.  Use this
     * function, and your communtication will be TCP-friendly, which is good.
     * Once the message is sent, or when <code>tries</code> timeouts have
     * occurred while trying to send it, the give callback will be called.
     *
     * @param msg             the message to send
     * @param dst             the message's destination host and port
     * @param tries           the number of attempts made to send the message
     * @param callback        called when the message is sent, or when the 
     *                        number of tries is exhausted
     * 
     * @return                a token than can be passed to cancelSend
     */
    public Object send (Object msg, InetSocketAddress dst,
	    long timeout_sec, Thunk1<Boolean> callback) {

	long now_ms = System.currentTimeMillis ();

	if (DEBUG) debugln ("got " + msg + " to send to " + dst);

	Connection conn = (Connection) conns.get (dst);
	if (conn == null) {
	    conn = new Connection (dst, now_ms);
	    conns.put (dst, conn);
	}

	TimeoutInfo tinfo = new TimeoutInfo (conn, msg, now_ms, 
                timeout_sec*1000, inc_next_msg_id(), callback);
	conn.send_q.addLast (tinfo);

        if (DEBUG) debugln ("conn for " + dst + ": send_q.size=" +
                conn.send_q.size () + ", retry_q.size=" + conn.retry_q.size ()
                + ", second_chance.size=" + second_chance.size ());

        if (conn.writable ())
            add_to_rr (conn);
        else if (DEBUG)
            debugln ("not yet writable");

        return new Long(tinfo.msg_id);
    }

    /**
     * Send a congestion-controlled message to another host.  Use this
     * function, and your communtication will be TCP-friendly, which is good.
     * Once the message is sent, or when <code>tries</code> timeouts have
     * occurred while trying to send it, the give callback will be called.
     *
     * @param msg             the message to send
     * @param dst             the message's destination host and port
     * @param tries           the number of attempts made to send the message
     * @param cb              the callback to call when the message is
     *                        sent, or when the number of tries is exhausted
     * @param user_data       the application-specific data to call that
     *                        callback with
     * 
     * @return                a token than can be passed to cancelSend
     */
    public Object send (Object msg, InetSocketAddress dst,
	    long timeout_sec, final SendCB cb, final Object user_data) {
        return send(msg, dst, timeout_sec, new Thunk1<Boolean>() {
                public void run(Boolean success) {
                    cb.cb(user_data, success.booleanValue());
                }
            });
    }

    /**
     * Cancel a send in progress.  When send is called, it returns a token; if
     * this function is called with that token, then the corresponding message
     * will not be sent if it hasn't already been sent, and the callback
     * passed to send will not be called if it hasn't already been called.
     */
    public void cancelSend(Object token) {
        if (token == null) throw new NullPointerException();
        cancelled.add((Long) token);
    }

    /**
     * Returns the number of milliseconds since the epoch of the last time we
     * sent a message to this peer, or 0 if we haven't sent a message to them
     * at all.
     */
    public long last_send (InetSocketAddress peer) {
        Connection conn = (Connection) conns.get (peer);
        if (conn == null)
            return 0;
        else
            return conn.lastsnd;
    }

    /**
     * Returns the number of milliseconds since the epoch of the last time we
     * received an acknowledgement from this peer, or 0 if we have yet to
     * receive an acknowledgement from them.
     */
    public long last_recv (InetSocketAddress peer) {
        Connection conn = (Connection) conns.get (peer);
        if (conn == null)
            return 0;
        else
            return conn.lastrcv;
    }

    /**
     * Returns the current estimate of the mean latency to this peer, or -1 if
     * there is no current estimate.
     */
    public long latency_mean (InetSocketAddress peer) {
        Connection conn = (Connection) conns.get (peer);
        if (conn == null)
            return -1L;
        else
            return conn.sa >> 3;
    }

    /**
     * Returns the number of messages waiting to be sent or currently in
     * flight to this peer.
     */
    public int queued_msgs (InetSocketAddress peer) {
        Connection conn = (Connection) conns.get (peer);
        if (conn == null)
            return 0;
        else
            return conn.retry_q.size () + conn.send_q.size () +
                conn.probe_q.size () + conn.inf.size ();
    }

    /**
     * Returns the number of messages waiting to be sent or currently in
     * flight to all peers.
     */
    public int queued_msgs () {
	int result = 0;
	Iterator i = conns.keySet ().iterator ();
	while (i.hasNext ()) {
	    InetSocketAddress peer = (InetSocketAddress) i.next ();
	    Connection conn = (Connection) conns.get (peer);
	    result += conn.retry_q.size () + conn.send_q.size () +
		conn.probe_q.size () + conn.inf.size ();
	}
	return result;
    }

    /**
     * Don't use this function; I'm trying to figure out how to get rid of it.
     */
    public LinkedList send_q (InetSocketAddress peer) {
        Connection conn = (Connection) conns.get (peer);
        if (conn == null)
            return null;
        else
            return conn.send_q;
    }

    public static class TimeoutInfo {
	public Object msg;
	public Connection conn;
	public long start_ms, send_ms;
        public long msg_id;
	public int attempt;
	public long timeout_ms;
	public boolean cut_ssthresh;
	public Thunk1<Boolean> send_cb;

	public TimeoutInfo (TimeoutInfo other) {
	    msg = other.msg;
	    conn = other.conn;
	    send_ms = other.send_ms;
	    attempt = other.attempt;
	    timeout_ms = other.timeout_ms;
	    cut_ssthresh = other.cut_ssthresh;
	    send_cb = other.send_cb;
            msg_id = other.msg_id;
	}

	public TimeoutInfo (Connection c, Object m, long n, long to,
		            long i, Thunk1<Boolean> scb) {
	    cut_ssthresh = true; conn = c; msg = m; attempt = 0;
	    start_ms = n; timeout_ms = to; msg_id = i;
	    send_ms = 0; send_cb = scb;
	}
    }

    /**
     * Keep track of all of the relavent information about another node we are
     * talking to.
     */
    protected class Connection {
	public InetSocketAddress addr;

        /**
         * The mean RTT, variance of the RTT, and the round-trip timeout to
         * this host, all in milliseconds.  The mean is scaled by a factor of
         * 8, and the variance is scaled by a factor of 4.  See [Jac88].
         */
	public long sa, sv, rto;

        public int consecutive_timeouts;

        /**
         * The congestion window size and slow-start threshold.  See [Jac88].
         */
	public double cwnd, ssthresh;

        /**
         * TimeoutInfo objects for each message in flight, indexed by sequence
         * number.
         */
	public Map inf = new HashMap ();

        /**
         * The sequence numbers of acknowledgements we need to send out.
         */
	public LinkedList ack_q = new LinkedList ();

        /**
         * TimeoutInfo objects for new messages to be sent out.
         */
	public LinkedList<TimeoutInfo> send_q = new LinkedList<TimeoutInfo>();

        /**
         * Non-conjestion controlled messages that need to be sent.
         */
	public LinkedList probe_q = new LinkedList ();

        /**
         * TimeoutInfo objects for messages that need to be resent.
         */
	public LinkedList<TimeoutInfo> retry_q = new LinkedList<TimeoutInfo>();

        /**
         * The time in milliseconds since the epoch since we last sent a
         * message to this host and since we last received an acknowledgement
         * from them.
         */
	public long lastsnd, lastrcv;

        /**
         * We use these to keep track of the time that the average message
         * spends in the send_q before being sent out on the wire.
         */
	public long time_to_first_send;
	public int time_to_first_send_cnt;

        /**
         * We use these to keep track of the time that the average message
         * takes to be acknowledged.
         */
	public long time_to_ack;
	public int time_to_ack_cnt;

        /**
         * We put messages on the wire from the send_q, ack_q, probe_q, and
         * retry_q in round-robin fashion, and this integer keeps track of
         * which one to pull out of next time we can write the socket.
         */
	public int next_q;

        /**
         * Which connection is after us in the round-robin queue?
         */
        public Connection next;

        /**
         * Are we currently in the round-robin queue?
         */
        public boolean in_rr;

	public Connection (InetSocketAddress a, long now_ms) {
	    addr = a; sa = -1; sv = 0; rto = MAX_RTO;
	    lastrcv = now_ms; // so we don't immediately think they're down
	    lastsnd = now_ms; // so we don't immediately throw them out
	    cwnd = 1.0; ssthresh = MAX_WND;
	}

        /**
         * Do we have a regular msg to send, and are we within the congestion
         * window?
         */
        public final boolean can_send_msg () {
            return ((! send_q.isEmpty ()) && (inf.size () < (int) cwnd));
        }

        /**
         * Do we have a regular msg to retry, and are we within the congestion
         * window?
         */
        public final boolean can_send_retry () {
            return ((! retry_q.isEmpty ()) && (inf.size () < (int) cwnd));
        }

        /**
         * Do we have a regular msg to send or retry, and are we within the
         * congestion window?
         */
        public final boolean can_send_either () {
            if (DEBUG) {
                if (send_q.isEmpty () && retry_q.isEmpty ())
                    debugln ("send_q and retry_q both empty");
                else if (inf.size () >= (int) cwnd)
                    debugln ("inf.size >= cwnd");
            }
            return (((! send_q.isEmpty ()) || (! retry_q.isEmpty ())) &&
                    (inf.size () < (int) cwnd));
        }

        /**
         * Do we have a nocc msg to send?
         */
        public final boolean can_send_probe () {
            return ! probe_q.isEmpty ();
        }

        /**
         * Do we have an ack to send?
         */
        public final boolean can_send_ack () {
            return ! ack_q.isEmpty ();
        }

        public final boolean writable () {
	    // If we still have something to send on this connection, and
	    // it's conjestion window is not yet full, or if we have an ack
	    // to send, it is writable.

	    if (! ack_q.isEmpty ())
		return true;

	    if (! probe_q.isEmpty ())
		return true;

	    if (retry_q.isEmpty () && send_q.isEmpty ())
		return false;

	    return inf.size () < (int) cwnd;
	}

	public final void add_rtt_meas (long m) {
	    long orig_m = m;
	    if (sa == -1) {
		// After the first measurement, set the timeout to four
		// times the RTT.

		sa = m << 3;
		sv = 0;
		rto = (m << 2) + 10; // the 10 is to accont for GC
	    }
	    else {
		m -= (sa >> 3);
		sa += m;
		if (m < 0)
		    m = -1*m;
		m -= (sv >> 2);
		sv += m;
		rto = (sa >> 3) + sv + 10; // the 10 is to accont for GC
	    }

	    // Don't backoff past 1 second.
	    if (rto > MAX_RTO) {
		if (DEBUG) debugln ("huge rto: conn=" + addr +
			" m=" + orig_m + " sa=" + sa + " sv=" + sv +
			" rto=" + rto);

		rto = MAX_RTO;
	    }

	    if (cwnd < ssthresh) // slow start
		cwnd += 1.0;
	    else		 // increment by one
		cwnd += 1.0 / cwnd;

	    if (cwnd > MAX_WND)
		cwnd = MAX_WND;
	}


	public final void timeout () {
	    rto <<= 1;

	    // Don't backoff past MAX_RTO.
	    if (rto > MAX_RTO)
		rto = MAX_RTO;

	    ssthresh = cwnd / 2.0;
	    cwnd = 1.0;
	}
    }

    protected class MySelectableCB implements ASyncCore.SelectableCB {
	public void select_cb (SelectionKey skey, Object user_data) {

            // We want to do any available reads first, because if a message
            // has come in, it may generate a response.  If so, we want it to
            // do so before we check for writability, so that if the socket is
            // writable, we can piggyback the ack for the incoming message on
            // the application-level response.

	    if (DEBUG) debugln ("MySelectableCB readable=" +
                    skey.isReadable () + ", writable=" +
                    skey.isWritable () + ", want write=" +
                    ((skey.interestOps () & SelectionKey.OP_WRITE) != 0));

	    if (skey.isReadable ()) {
		handle_readable ();
	    }

            // UdpCC.close () may have been called in the last handle_inb_msg
            // of handle readable.
            if (closed) return;

	    if (((skey.interestOps () & SelectionKey.OP_WRITE) != 0)
		    && skey.isWritable ()) {

		handle_writable ();
	    }
	}
    }

    protected static class TimeoutInfoAndSeq {
        public TimeoutInfo tinfo;
        public Long seq;
        public TimeoutInfoAndSeq (TimeoutInfo t, Long s) { tinfo = t; seq = s; }
    }

    protected class MyAckTimeoutCB implements ASyncCore.TimerCB {
	public void timer_cb (Object user_data) {
            if (closed) return;

	    Long seq = (Long) user_data;
	    TimeoutInfo tinfo = (TimeoutInfo) unacked.remove (seq);
	    if (DEBUG) debugln ("check timeout "
                        + Long.toHexString (seq.longValue ()));

            long now_ms = System.currentTimeMillis ();
	    if (tinfo == null) {
		// already acked

		if (DEBUG) debugln ("seq 0x"
                        + Long.toHexString (seq.longValue ())
                        + " already acked");
	    }
	    else {
		// timeout

                TimeoutInfo scti = new TimeoutInfo (tinfo);
		second_chance.put (seq, scti);
                second_chance_timeouts.add (
                        new TimeoutInfoAndSeq (scti, seq), now_ms);

		if (DEBUG_RTT) debugln ("timeout seq 0x"
                        + Long.toHexString (seq.longValue ())
                        + ", peer=" + tinfo.conn.addr
                        + ", rtt=" + (now_ms - tinfo.send_ms)
                        + ", now=" + now_ms);

		if (tinfo.cut_ssthresh) {
                    ++tinfo.conn.consecutive_timeouts;
		    tinfo.conn.timeout ();
		    Iterator j = tinfo.conn.inf.keySet ().iterator ();
		    while (j.hasNext ()) {
			Long s2 = (Long) j.next ();
			TimeoutInfo t2 = (TimeoutInfo) tinfo.conn.inf.get (s2);
			t2.cut_ssthresh = false;
		    }
		}
		tinfo.conn.inf.remove (seq);

                LinkedList to_callback = null;

                if (tinfo.start_ms + tinfo.timeout_ms > now_ms)
                    tinfo.conn.retry_q.addLast (tinfo);
                else {
                    to_callback = new LinkedList ();
                    to_callback.addLast (tinfo);
                }

                for (int queue = 0; queue < 2; ++queue) {
                    Iterator i = (queue == 0)
                        ? tinfo.conn.retry_q.iterator ()
                        : tinfo.conn.send_q.iterator ();
                    while (i.hasNext ()) {
                        TimeoutInfo tmp = (TimeoutInfo) i.next ();
                        if (tmp.start_ms + tmp.timeout_ms <= now_ms) {
                            i.remove ();
                            if (to_callback == null)
                                to_callback = new LinkedList ();
                            to_callback.addLast (tmp);
                        }
                    }
                }

                if (to_callback != null) {
                    if (DEBUG_MIN) debugln ("After " +
                            tinfo.conn.consecutive_timeouts +
                            " consecutive timeouts to " + tinfo.conn.addr +
                            ", cancelling the following messages:");
                    while (! to_callback.isEmpty ()) {
                        TimeoutInfo tmp =
                            (TimeoutInfo) to_callback.removeFirst ();
                        if (DEBUG_MIN) debugln ("    " + tmp.msg);
                        if ((!cancelled.remove(new Long(tmp.msg_id))) 
                            && (tmp.send_cb != null)) {
                            tmp.send_cb.run(new Boolean(false)/*failure*/);
                        }
                    }
                }

		if (tinfo.conn.writable ())
		    add_to_rr (tinfo.conn);
	    }

            // Clean out the second_chance set.

            while ((! second_chance_timeouts.isEmpty ()) &&
                   (second_chance_timeouts.getFirstPriority ()
                    + 60*1000 < now_ms)) {

                TimeoutInfoAndSeq p =
                    (TimeoutInfoAndSeq) second_chance_timeouts.removeFirst ();
		second_chance.remove (p.seq);
            }
	}
    }

    protected class BandwidthCB implements  ASyncCore.TimerCB {
	public void timer_cb (Object user_data) {
            long now_ms = System.currentTimeMillis ();
            if (DEBUG_MIN) logger.info (
                    " ib=" + in_bytes + " ip=" + in_pkts +
                    " ob=" + out_bytes + " op=" + out_pkts);
            in_bytes = in_pkts = out_bytes = out_pkts = 0;

            if (! closed)
                acore.register_timer (BW_STATS_PERIOD, this, null);
        }
    }

    protected class StatsCB implements ASyncCore.TimerCB {
	public void timer_cb (Object user_data) {
	    long now_ms = System.currentTimeMillis ();
	    Iterator i = conns.keySet ().iterator ();
	    while (i.hasNext ()) {
		InetSocketAddress addr = (InetSocketAddress) i.next ();
		Connection conn = (Connection) conns.get (addr);

		double ttfs = (double) conn.time_to_first_send;
		if (conn.time_to_first_send_cnt > 0)
		    ttfs /= conn.time_to_first_send_cnt;
		else
		    ttfs = 0.0;
		conn.time_to_first_send = 0;
		conn.time_to_first_send_cnt = 0;

		double tta = (double) conn.time_to_ack;
		if (conn.time_to_ack_cnt > 0)
		    tta /= conn.time_to_ack_cnt;
		else
		    tta = 0.0;
		conn.time_to_ack = 0;
		conn.time_to_ack_cnt = 0;

		if (conn.lastrcv + 30*1000 > now_ms) {
		    logger.info (addr +
			    " sa=" + (conn.sa >> 3) +
			    " sv=" + (conn.sv >> 2) +
			    " rto=" + conn.rto +
			    " cwnd=" + conn.cwnd +
			    " ssthresh=" + conn.ssthresh +
			    " ttfs=" + ttfs +
			    " tta=" + tta);
		}
	    }

            if (! closed)
                acore.register_timer (STATS_PERIOD, this, null);
	}
    }

    protected boolean USE_DIRECT = true;
    protected static final long MAX_RTO = 5*1000;
    protected static final long BW_STATS_PERIOD = 5*1000;
    protected static final long STATS_PERIOD = 30*1000;
    protected static final double MAX_WND = 1000.0*1000.0;
    protected static final int MAX_MSG_SIZE = 16*1024;
    protected static final boolean LOG_BAD_NETWORK_EVENTS = true;
    protected static final boolean REUSE = true;

    protected double timeout_factor = 1.0, timeout_diff = 0.0;

    protected boolean closed;
    protected ASyncCore acore;
    protected Serializer serializer;
    protected Sink sink;
    protected MyAckTimeoutCB ack_timeout_cb = new MyAckTimeoutCB ();
    protected SelectionKey skey;
    protected DatagramChannel channel;
    protected DatagramSocket sock;
    protected InetSocketAddress my_addr;
    protected Map unacked = new HashMap ();
    protected Map second_chance = new HashMap ();
    protected PriorityQueue second_chance_timeouts = new PriorityQueue (10);
    protected long next_msg_id;
    protected Connection rr_first, rr_last;
    protected Set<Long> cancelled = new HashSet<Long>();
    protected Map conns = new HashMap ();
    protected ByteBuffer reuse_buf = USE_DIRECT
	    ? ByteBuffer.allocateDirect (MAX_MSG_SIZE)
	    : ByteBuffer.allocate (MAX_MSG_SIZE);
    protected long in_bytes, in_pkts, out_bytes, out_pkts;
    protected HashSet recently_seen_set = new HashSet ();
    protected LinkedList recently_seen_list = new LinkedList ();
    protected static final int MAX_RECENTLY_SEEN_SIZE = 1000;

    protected void debugln (String msg) {
        logger.debug (msg);
    }

    protected static class SrcAndMsgId {
        public InetSocketAddress src;
        public long msg_id;
        public SrcAndMsgId (InetSocketAddress s, long m) {
            src = s; msg_id = m;
        }
        public int hashCode () {
            return src.hashCode () ^ (int) msg_id;
        }
        public boolean equals (Object rhs) {
            SrcAndMsgId other = (SrcAndMsgId) rhs;
            return (msg_id == other.msg_id) && src.equals (other.src);
        }
    }

    protected final boolean recently_seen (InetSocketAddress src, long msg_id) {
        SrcAndMsgId k = new SrcAndMsgId (src, msg_id);
        ++received_msgs;
        if (recently_seen_set.contains (k)) {
            ++received_duplicates; 
            return true;
        }
        else {
            recently_seen_list.addLast (k);
            recently_seen_set.add (k);
            if (recently_seen_list.size () > MAX_RECENTLY_SEEN_SIZE)
                recently_seen_set.remove (recently_seen_list.removeFirst ());
            return false;
        }
    }

    protected final long inc_next_msg_id () {
        if (next_msg_id < 0)
            BUG ("next_msg_id=" + Long.toHexString (next_msg_id));
        if (next_msg_id == (Long.MAX_VALUE >>> 8))
            next_msg_id = 0;
        else
            ++next_msg_id;
        return next_msg_id;
    }

    protected final long msg_id (long seq) {
        if (seq < 0)
            throw new IllegalArgumentException (
                    "seq=" + Long.toHexString (seq) + " in msg_id");
        return seq >> 8;
    }

    protected final int attempt (long seq) {
        if (seq < 0)
            throw new IllegalArgumentException (
                    "seq=" + Long.toHexString (seq) + " in make_seq");
        return (int) (seq & 0x7fL);
    }

    protected final long make_seq (long msg_id, int attempt) {
        if (msg_id > (Long.MAX_VALUE >> 8))
            throw new IllegalArgumentException (
                    "msg_id=" + Long.toHexString (msg_id) + " in make_seq");
        if (attempt > 0x7f)
            throw new IllegalArgumentException (
                    "attempt=" + Long.toHexString (attempt) + " in make_seq");
        long seq = (msg_id << 8) | attempt;
        if (seq < 0)
            BUG ("seq=" + Long.toHexString (seq) + " in make_seq");
        return seq;
    }

    protected void BUG (Exception e) {
        logger.fatal ("unhandled exception", e);
	System.exit (1);
    }

    protected void BUG (String msg) {
        Exception e = null;
        try { throw new Exception (); } catch (Exception c) { e = c; }
	logger.fatal (msg, e);
	System.exit (1);
    }

   protected final ByteBuffer alloc_bb (int sz) {
	if (REUSE) {
	    reuse_buf.clear ();
	    reuse_buf.limit (sz);
	    return reuse_buf;
	}
	else {
	    if (USE_DIRECT)
		return ByteBuffer.allocateDirect (sz);
	    else
		return ByteBuffer.allocate (sz);
	}
    }

    protected final void handle_ack (Long seq) {
	TimeoutInfo tinfo = (TimeoutInfo) unacked.remove (seq);
	long now_ms = System.currentTimeMillis ();

	if (tinfo == null) {
	    tinfo = (TimeoutInfo) second_chance.remove (seq);
	    if (tinfo == null) {
		if (DEBUG) debugln ("got unexpected ack for seq 0x" +
                        Long.toHexString (seq.longValue ()));
	    }
	    else {
		tinfo.conn.lastrcv = System.currentTimeMillis ();

		if (DEBUG_RTT) debugln ("2nd chance ack seq 0x"
                        + Long.toHexString (seq.longValue ())
                        + ", peer=" + tinfo.conn.addr
                        + ", rtt=" + (now_ms - tinfo.send_ms)
                        + ", now=" + now_ms);

		tinfo.conn.add_rtt_meas (
			System.currentTimeMillis () - tinfo.send_ms);
	    }
	    return;
	}
	Connection conn = tinfo.conn;

	conn.time_to_ack += (now_ms - tinfo.start_ms);
	conn.time_to_ack_cnt += 1;

	conn.lastrcv = now_ms;

	if (DEBUG) debugln ("got ack for seq 0x" +
                Long.toHexString (seq.longValue ()));

	conn.add_rtt_meas (
		System.currentTimeMillis () - tinfo.send_ms);
	conn.inf.remove (seq);

	if (DEBUG) debugln ("conn=" + conn.addr + " cwnd=" +
		((int) conn.cwnd) + " inf=" + conn.inf.size ());

        tinfo.conn.consecutive_timeouts = 0;

	if (conn.writable ())
	    add_to_rr (conn);

        if ((!cancelled.remove(new Long(tinfo.msg_id))) 
            && (tinfo.send_cb != null)) {
                tinfo.send_cb.run(new Boolean(true)/* success */);
        }
    }

    protected final void handle_inb_msg (ByteBuffer bb, InetSocketAddress src) {

	long ack = bb.getLong ();
	Long seq = new Long (bb.getLong ());
        long wait_ms = -1L;
        long est_rtt_ms = -1L;
        if (bb.limit () - bb.position () >= 8) {
            wait_ms = bb.getInt ();
            est_rtt_ms = bb.getInt ();
        }
        int size = bb.limit() - bb.position();

        if (ack != -1L)
            handle_ack (new Long (ack));

	long now_ms = System.currentTimeMillis ();
	Connection conn = (Connection) conns.get (src);
	if (conn == null) {
	    conn = new Connection (src, now_ms);
	    conns.put (src, conn);
	}

	// Send an ack.
        int tries = -1;
	if (seq.longValue () != -1L) {
	    conn.ack_q.addLast (seq);
            tries = attempt (seq.longValue ());
        }

	if (conn.writable ())
	    add_to_rr (conn);

        if ((seq.longValue () != -1L) &&
            recently_seen (src, msg_id (seq.longValue ()))) {
            if (DEBUG) debugln ("received duplicate for msg_id=0x"
                    + Long.toHexString (msg_id (seq.longValue ())));
        }
        else {

            Object msg = null;
            try {
                msg = serializer.deserialize (bb);
            }
            catch (Exception e) {
		if (DEBUG_MIN) {
		    logger.debug ("caught " + e +
			    " deserializing a message from " + src, e);
                }
                return;
            }
            catch (OutOfMemoryError e) {
                System.gc ();
                StringBuffer bytes = new StringBuffer (bb.position () * 3);
                bb.position (0);
                int col = 0;
                while (bb.position () < bb.limit ()) {
                    String bs = Integer.toHexString (bb.get () & 0xff);
                    if (bs.length () == 1) bs = "0" + bs;
                    bytes.append (bs);
                    if (++col == 24) {
                        bytes.append ("\n");
                        col = 0;
                    }
                    else if ((col > 0) && (col % 4 == 0))
                        bytes.append (" ");
                }
                logger.error ("out of memory error deserializing bytes:\n"
                        + bytes);
            }

            if (DEBUG && (seq.longValue () != -1L)) {
                if (DEBUG_MIN) debugln ("received " + msg + " seq=0x" +
                        Long.toHexString (seq.longValue ()) + " from " + src);
	    }

            if (!conn.addr.equals(my_addr))
                countBytes(msg, size);

            sink.recv (msg, src, my_addr, tries, wait_ms, est_rtt_ms);
        }
    }

    protected final void handle_readable () {
        outer:
	while (true) {

	    ByteBuffer bb = alloc_bb (MAX_MSG_SIZE);
	    InetSocketAddress src = null;
	    try {
		src = (InetSocketAddress) channel.receive (bb);
	    }
	    catch (SocketException e) {
                // For some reason, Sun's j2sdk1.4.2 will occasionally throw
                // one of these with the message "Connection reset by peer: no
                // further information".  That doesn't make any sense to me,
                // but I think we can safely ignore it.  We'll get the data
                // for the next packet on the next select loop.

                return;
	    }
	    catch (IOException e) {
		BUG (e);
	    }

	    if (bb.position () == 0)
		return;

	    bb.flip ();

            in_pkts += 1;
            in_bytes += bb.limit () + 20 /* account for IP header */;
            
            int protocol_version = bb.getInt ();

            if (protocol_version == 0) {
                // This is the new, un-MAC'ed format.  If a MAC is set, 
                // ignore the message.

                if (mac != null) {
                    logger.info ("un-MAC'ed message from " 
                            + src.getAddress ().getHostAddress ());
                    continue;
                }

                if (bb.limit () < 12) {
                    logger.info ("message < 12 bytes from "
                            + src.getAddress ().getHostAddress ());
                    continue;
                }

                if (bb.limit () == 12)
                    handle_ack (new Long (bb.getLong ()));
                else
                    handle_inb_msg (bb, src);
            }
            else if (protocol_version == 1) {
                // This the the new, MAC'ed format.  If no MAC is set on this
                // node, ignore the MAC included in the message.  Otherwise,
                // check the MAC.

                if (bb.limit () < 32) {
                    logger.info ("message < 32 bytes from "
                            + src.getAddress ().getHostAddress ());
                    continue;
                }

                if (mac != null) {
                    assert bb.hasArray ();
                    // The MAC is over the entire packet except the MAC
                    // itself, which is the last 20 bytes.

                    mac.update (bb.array(), bb.arrayOffset(), bb.limit() - 20);
                    byte [] macbytes = mac.doFinal();
                    int j = bb.arrayOffset () + bb.limit () - 20;
                    for (int i = 0; i < 20; ++i) {
                        if (macbytes [i] != bb.array() [j++]) {
                            // Don't match
                            logger.info ("macs don't match addr=" 
                                    + src.getAddress ().getHostAddress ());
                            continue outer;
                        }
                    }
                }

                if (bb.limit () == 32) 
                    handle_ack (new Long (bb.getLong ()));
                else
                    handle_inb_msg (bb, src);
            }
            else {
                logger.info ("unknown protocol 0x" 
                        + Integer.toHexString (protocol_version) + " from " 
                        + src.getAddress ().getHostAddress ());
                continue;
            }

            // UdpCC.close () may have been called from the user's handler
            // in handle_ack or handle_inb_msg.
            if (closed) return;
	}
    }

    protected final boolean send_ack (Connection conn) {
	long now_ms = System.currentTimeMillis ();
	Long seq = (Long) conn.ack_q.getFirst ();

	if (DEBUG) debugln ("sending ack seq 0x" +
                Long.toHexString (seq.longValue ()));

        ByteBuffer bb = null;
        if (mac == null) {
            bb = alloc_bb (12);
            bb.putInt (0); // protocol version
            bb.putLong (seq.longValue ());
            bb.rewind ();
        }
        else {
            bb = alloc_bb (32);
            bb.putInt (1); // protocol version
            bb.putLong (seq.longValue ());
            mac.update (bb.array(), bb.arrayOffset(), bb.position ());
            byte [] macbytes = mac.doFinal();
            bb.put (macbytes, 0, 20);
            bb.rewind ();
        }

	if (DEBUG) debugln ("acking " + seq);

	int n = 0;
	try {
	    n = channel.send (bb, conn.addr);
	}
	catch (IOException e) {
	    BUG (e);
	}

	if (n == 0)
	    return false;

        // Send was successful.

        out_pkts += 1;
        out_bytes += bb.limit () + 20 /* account for IP header */;

	conn.ack_q.removeFirst ();
	return true;
    }

    protected final boolean send_probe (Connection conn) {
	long seq = -1L;

	Object msg = conn.probe_q.getFirst ();

	if (DEBUG) debugln ("sending probe");

        // Piggyback an ACK if we have one.
        long ack = -1L;
        if (! conn.ack_q.isEmpty ()) {
            ack = ((Long) conn.ack_q.getFirst ()).longValue ();
            if (DEBUG) debugln ("piggybacking ack " + ack);
        }

        int sz = serializer.serialize_size (msg)
            + 28 /* for version+ack+seq+wait_ms+est_rtt_ms */;
	if (sz > MAX_MSG_SIZE) {
	    BUG ("size=" + sz + " is greater than max size=" + MAX_MSG_SIZE +
		    " for msg " + msg);
	}

	long now_ms = System.currentTimeMillis ();

	ByteBuffer bb = null;
        if (mac == null) {
            bb = alloc_bb (sz);
            bb.putInt (0); // protocol version
        }
        else {
            bb = alloc_bb (sz + 20);
            bb.putInt (1); // protocol version
        }
        bb.putLong (ack);
        bb.putLong (seq);
        bb.putInt ((int) 0); // don't know how long it's been waiting
        bb.putInt ((int) (conn.sa >> 3));
        serializer.serialize (msg, bb);
	bb.rewind ();

	int n = 0;
	try {
	    n = channel.send (bb, conn.addr);
	}
	catch (IOException e) {
	    BUG (e);
	}

	if (n == 0) {
	    // Undo
	    if (DEBUG) debugln ("send failed, will retry later");
	    return false;
	}

        // Send was successful.

        if (ack != -1)
            conn.ack_q.removeFirst ();

        out_pkts += 1;
        out_bytes += bb.limit () + 20 /* account for IP header */;

	conn.probe_q.removeFirst ();
	return true;
    }

    protected final boolean send_msg (Connection conn, boolean retry) {

        TimeoutInfo tinfo = null;
        LinkedList<TimeoutInfo> q = retry ? conn.retry_q : conn.send_q;
        while (!q.isEmpty()) {
            TimeoutInfo t = q.getFirst();
            if (cancelled.remove(new Long(t.msg_id)))
                q.removeFirst();
            else {
                tinfo = t; 
                break;
            }
        }
        if (tinfo == null)
            return true; // socket still writable

        tinfo.attempt++;
	long seq = make_seq (tinfo.msg_id, tinfo.attempt);

	if (DEBUG) debugln ("sending " + tinfo.msg + " seq=0x" +
                Long.toHexString (seq));

        // Piggyback an ACK if we have one.
        long ack = -1L;
        if (! conn.ack_q.isEmpty ()) {
            ack = ((Long) conn.ack_q.getFirst ()).longValue ();
            if (DEBUG) debugln ("piggybacking ack " + ack);
        }

	long now_ms = System.currentTimeMillis ();
        long wait_ms = now_ms - tinfo.start_ms;

	int sz = serializer.serialize_size (tinfo.msg)
            + 28 /* for version+ack+seq+wait_ms+est_rtt_ms */;
	if (sz > MAX_MSG_SIZE) {
	    BUG ("size=" + sz + " is greater than max size=" + MAX_MSG_SIZE +
		    " for msg " + tinfo.msg);
	}

	ByteBuffer bb = null;
        if (mac == null) {
            bb = alloc_bb (sz);
            bb.putInt (0); // protocol version
        }
        else {
            bb = alloc_bb (sz + 20);
            bb.putInt (1); // protocol version
        }
	bb.putLong (ack);
        bb.putLong (seq);
        bb.putInt ((int) wait_ms);
        bb.putInt ((int) (conn.sa >> 3));
	serializer.serialize (tinfo.msg, bb);
        if (mac != null) {
            mac.update (bb.array(), bb.arrayOffset(), bb.position ());
            byte [] macbytes = mac.doFinal();
            bb.put (macbytes, 0, 20);
        }
	bb.rewind ();

	int n = 0;
	try {
	    n = channel.send (bb, conn.addr);
	}
	catch (IOException e) {
            bb.rewind ();
            byte [] data = new byte [bb.limit () - bb.position ()];
            bb.get (data, 0, data.length);
	    logger.fatal ("bb.pos=" + bb.position () + " bb.lim=" 
                          + bb.limit () + " addr=" 
                          + conn.addr.getAddress ().getHostAddress ()
                          + " msg=\n" + StringUtil.bytes_to_str(data), e);
            System.exit (1);
	}

	if (n == 0) {
	    // Undo
	    if (DEBUG) debugln ("send failed, will retry later");
	    return false;
	}

        // Send was successful.

        out_pkts += 1;
        out_bytes += bb.limit () + 20 /* account for IP header */;

        if (ack != -1)
            conn.ack_q.removeFirst ();

        if (retry) {
            conn.retry_q.removeFirst ();
            TimeoutInfo tnew = new TimeoutInfo (
                    conn, tinfo.msg, tinfo.start_ms, tinfo.timeout_ms,
                    tinfo.msg_id, tinfo.send_cb);
            tnew.attempt = tinfo.attempt;
            tinfo = tnew;
        }
        else {
            conn.send_q.removeFirst ();
            conn.time_to_first_send += (tinfo.send_ms - tinfo.start_ms);
            conn.time_to_first_send_cnt += 1;
        }
        tinfo.send_ms = now_ms;

	Long Seq = new Long (seq);
        unacked.put (Seq, tinfo);
	conn.inf.put (Seq, tinfo);
	conn.lastsnd = now_ms;

        long timeout_ms = Math.round (conn.rto * timeout_factor + timeout_diff);
	if (DEBUG) debugln ("setting timeout for " + timeout_ms);
	acore.register_timer (timeout_ms, ack_timeout_cb, Seq);

	return true;
    }

    protected final void add_to_rr (Connection conn) {
	// A connection can be added to the round robin queue waiting on
	// writability, but then no longer be writable when it gets its turn,
	// because in the interim a timeout has occurred and cwnd has been
	// lowered.  As a result, conn.writable is not a good indicator of
	// whether a connection is in the round robin queue already or
	// not.  Instead, we check conn.in_rr.

	if (! conn.in_rr) {
	    if (DEBUG) debugln ("adding conn=" + conn.addr + " to rr");
            conn.in_rr = true;
            if (rr_first == null)
                rr_first = rr_last = conn;
            else {
                rr_last.next = conn;
                rr_last = conn;
            }
            conn.next = null;
	    skey.interestOps (
		    skey.interestOps () | SelectionKey.OP_WRITE);
	}
	else {
	    if (DEBUG) debugln ("conn=" + conn.addr + " already in rr");
	}
    }

    protected void handle_writable () {

        // For some reason, when the round-robin queue was implemented with
        // java.util.LinkedList, the removeFirst statement kept throwing a
        // NoSuchElement exception which--given that it immediately followed a
        // !isEmpty check in a single-threaded system--made no sense.  So I've
        // implemented the list by hand and it seems to have made the problem
        // go away.  I love compiler bugs...

	while (rr_first != null) {

	    Connection conn = rr_first;
            rr_first = rr_first.next;
            conn.in_rr = false;

	    if (conn.writable ()) {
		if (DEBUG) debugln ("handle_writable conn=" + conn.addr);

                if (conn.can_send_either () || conn.can_send_probe ()) {

                    // If we have anything other than an ack to send, send
                    // it, since the ack will get piggybacked along.

                    // Cycle between congestion-controlled and non-cc msgs,
                    // but only send one of the two before going on to the
                    // next connection in the rr queue.

                    boolean done = false;
                    while (! done) {
                        switch (conn.next_q) {
                            case 0:
                                // Try the retry queue first.

                                if (conn.can_send_retry ()) {
                                    if (! send_msg (conn, true /* retry */))
                                        return;
                                    done = true;
                                }
                                else if (conn.can_send_msg ()) {
                                    if (! send_msg (conn, false /* !retry */))
                                        return;
                                    done = true;
                                }
                                else if (DEBUG) {
                                    if (conn.send_q.isEmpty () &&
                                        conn.retry_q.isEmpty ())
                                        debugln ("no msgs");
                                    else
                                        debugln ("cwnd full");
                                }
                                break;
                            case 1:
                                if (conn.can_send_probe ()) {
                                    if (! send_probe (conn))
                                        return;
                                    done = true;
                                }
                                else {
                                    if (DEBUG) debugln ("no probes");
                                }
                                break;
                        }

                        conn.next_q = (conn.next_q + 1) % 2;
                    }
                }
                else {
                    if (conn.can_send_ack ()) {
                        if (! send_ack (conn))
                            return;
                    }
                    else if (DEBUG) debugln ("no acks");
                }
            }

	    // If the connection is still writable, leave it in the round
	    // robin list.

	    if (conn.writable ()) {
		if (DEBUG) debugln ("still writable");
		add_to_rr (conn);
	    }

	    // If there is nothing in the round robin list, stop selecting
	    // for writability.

	    if (rr_first == null) {
		if (DEBUG) debugln ("rr empty");
		skey.interestOps (SelectionKey.OP_READ);
	    }
	    else {
		if (DEBUG) debugln ("rr not empty");
	    }
	}
    }

    /////////////////////////////////////////////////////////////////
    //
    //                         Byte Counts
    //
    /////////////////////////////////////////////////////////////////

    public interface ByteCount {
        public boolean recordByteCount();
        public String byteCountKey();
    }

    protected Map<String,Pair<Integer,Integer>> byteCounts =
        new LinkedHashMap<String,Pair<Integer,Integer>>();

    protected Runnable printBytes = new Runnable() {
        public void run() {
            for (String key : byteCounts.keySet()) {
                Pair<Integer,Integer> p = byteCounts.get(key);
                logger.info("bytecount " + key + " " + p.first + " " + p.second);
            }
            byteCounts.clear();
            acore.registerTimer(10*1000, this);
        }
    };

    protected void countBytes(String key, int size) {
        Pair<Integer,Integer> p = byteCounts.get(key);
        int count = 0, sum = 0;
        if (p != null) {
            count = p.first.intValue();
            sum = p.second.intValue();
        }
        byteCounts.put(key, 
                Pair.create(new Integer(count + 1), new Integer(sum + size)));
    }

    protected void countBytes(Object msg, int size) {
        if (msg instanceof ByteCount) {
            ByteCount cm = (ByteCount) msg;
            if (cm.recordByteCount()) countBytes(cm.byteCountKey(), size);
        }
    }
}

