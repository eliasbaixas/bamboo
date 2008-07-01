/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.db;

import bamboo.lss.ASyncCore;
import bamboo.lss.NioInputBuffer;
import bamboo.lss.NioOutputBuffer;
import bamboo.util.GuidTools;
import bamboo.util.StandardStage;
import com.sleepycat.db.*;
import java.io.File;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QuickSerializable;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import seda.sandStorm.api.SinkException;
import seda.sandStorm.api.SinkIF;
import static bamboo.util.StringUtil.*;

/**
 * An asynchronous interface to BerkeleyDB.
 *
 * <p>Data is stored on disk in a table with the following fields:
 * <table>
 * <tr><td>Bytes</td><td>Data</td></tr>
 * <tr><td> 0-7 </td><td>put time since the epoch (microseconds)</td></tr>
 * <tr><td> 8-11</td><td>ttl interval after put time (seconds)</td></tr>
 * <tr><td> 12-31</td><td>guid</td></tr>
 * <tr><td> 32-51</td><td>data hash</td></tr>
 * <tr><td> 52  </td><td>whether this is a put (1) or remove (0)</td></tr>
 * <tr><td> 53- </td><td>data</td></tr>
 * </table>
 * The data hash is needed to guarentee a consistent scan order and for
 * removes.  The primary key is bytes 0-52, and the secondary key is the guid
 * concatenated with the data hash.
 *
 * @author Sean C. Rhea
 * @version $Id: StorageManager.java,v 1.77 2006/03/02 21:59:39 srhea Exp $
 */
public class StorageManager extends StandardStage
implements SingleThreadedEventHandlerIF {

    public static final BigInteger ZERO_GUID = BigInteger.ZERO;
    public static final byte [] ZERO_HASH = new byte [20];
    public static final InetAddress ZERO_CLIENT = bytes_to_addr (new byte [4]);
    public static final InetAddress MAX_CLIENT =
        bytes_to_addr (new byte [] {127, 127, 127, 127});
    public static final Key ZERO_KEY =
        new Key (0, 0, ZERO_GUID, ZERO_HASH, ZERO_HASH, false, ZERO_CLIENT);

    static final InetAddress bytes_to_addr (byte [] c) {
        InetAddress result = null;
        try {
            result = InetAddress.getByAddress(c);
        }
        catch (UnknownHostException e) {
            assert false;
        }
        return result;
    }

    public interface StorageMonitor {
        void storage_changed (boolean added, InetAddress client_id, long size);
    }

    protected boolean print_open_cursors = false;

    protected Set storage_monitors = new HashSet ();

    public static class AddMonitor implements QueueElementIF {
        public StorageMonitor monitor;
        public AddMonitor (StorageMonitor m) { monitor = m; }
    }

    public static class RemoveMonitor implements QueueElementIF {
        public StorageMonitor monitor;
        public RemoveMonitor (StorageMonitor m) { monitor = m; }
    }

    public void register_monitor (StorageMonitor s) {
        handleEvent (new AddMonitor (s));
    }

    public void unregister_monitor (StorageMonitor s) {
        handleEvent (new RemoveMonitor (s));
    }

    public void handle_add_monitor (AddMonitor event) {
        storage_monitors.add(event.monitor);
        if (logger.isDebugEnabled())
            logger.debug ("added monitor " + event.monitor);
        Dbc c = open_cursor (client_counts, null, "handle_add_monitor");
        int retval = 0;
        Dbt key = new Dbt ();
        Dbt data = new Dbt ();
        try { retval = c.get (key, data, Db.DB_FIRST); }
        catch (DbException e) { BUG(e); }
        while (retval != Db.DB_NOTFOUND) {
            assert retval == 0 : retval;
            InetAddress client = bytes_to_addr (key.getData ());
            long value = ByteBuffer.wrap(data.getData(), data.getOffset(),
                                         data.getSize()).getLong ();
            notify_storage_changed (event.monitor, true, client, value);
            try { retval = c.get (key, data, Db.DB_NEXT); }
            catch (DbException e) { BUG(e); }
        }
        close_cursor (c);
    }

    public void handle_remove_monitor (RemoveMonitor event) {
        storage_monitors.remove(event.monitor);
        if (logger.isDebugEnabled())
            logger.debug ("removed monitor " + event.monitor);
    }

    protected void storage_changed (final boolean added, final Key fk, 
                                    final int size, DbTxn xact) {

        assert xact != null;

        Dbt key = new Dbt (fk.client_id.getAddress(), 0, 4);
        Dbt old = new Dbt ();
        int retval = 0;
        try { retval = client_counts.get (xact, key, old, 0); }
        catch (DbException e) { BUG(e); }

        long old_usage = 0;
        if (retval != Db.DB_NOTFOUND) {
            ByteBuffer bb = ByteBuffer.wrap (old.getData (), old.getOffset (),
                                             old.getSize ());
            old_usage = bb.getLong ();
        }
        long new_usage = old_usage;
        if (added) 
            new_usage += size + Key.SIZE;
        else
            new_usage -= size + Key.SIZE;

        byte [] new_bytes = new byte [8];
        ByteBuffer bb = ByteBuffer.wrap (new_bytes);
        bb.putLong (new_usage);

        if (logger.isInfoEnabled()) {
            StringBuffer buf = new StringBuffer(256);
            buf.append("client ");
            buf.append(fk.client_id.getHostAddress());
            buf.append(", ");
            buf.append((added ? "+" : "-"));
            buf.append(size + Key.SIZE);
            buf.append(" bytes, old=");
            byte_cnt_to_sbuf(old_usage, buf);
            buf.append(", new=");
            byte_cnt_to_sbuf(new_usage, buf);
            logger.info(buf);
        }

        Dbt newval = new Dbt (new_bytes);
        try { retval = client_counts.put (xact, key, newval, 0); }
        catch (DbException e) { BUG(e); }

        Iterator si = storage_monitors.iterator();
        while (si.hasNext()) {
            notify_storage_changed ((StorageMonitor) si.next(),
                    added, fk.client_id, size + Key.SIZE);
        }
    }

    public void notify_storage_changed (final StorageMonitor sm, 
                                        final boolean added, 
                                        final InetAddress client_id, 
                                        final long size) {
        // Get back into main thread.
        acore.registerTimer(0, new Runnable() {
            public void run() { sm.storage_changed(added, client_id, size); }
        });
    }

    /**
     * The primary key under which data are stored--must be unique.
     */
    public static class Key implements Comparable {
	public static final int SIZE = 77;
	public long time_usec;
	public int ttl_sec; // expiry time == time_usec + ttl_sec
	public BigInteger guid;
	public byte [] secret_hash;
	public byte [] data_hash;
	public boolean put; // as opposed to remove
        public InetAddress client_id;
	public Key (long t, int tt, BigInteger g, byte [] sh, byte [] dh,
                    boolean p, InetAddress c) {
	    time_usec = t; ttl_sec = tt; guid = g; data_hash = dh; put = p; 
            client_id = c;
            if (sh == null)
                secret_hash = ZERO_HASH; 
            else
                secret_hash = sh; 
	}

        public long expiryTime() {
            return time_usec + ((long) ttl_sec) * 1000000;
        }

        public int ttlRemaining(long now_ms) {
            return (int) ((expiryTime() - now_ms * 1000) / 1000000);
        }

        public int hashCode () { return guid.hashCode(); }

	public Key (ByteBuffer buf) { this (new NioInputBuffer(buf), true); }

	protected Key (Dbt dbt) { 
            this (new NioInputBuffer(ByteBuffer.wrap(
                            dbt.getData(), dbt.getOffset(), dbt.getSize())),
                  true);
        }
  
        public Key (InputBuffer buf) { this(buf, false); }

        public Key (InputBuffer buf, boolean onDisk) {
            time_usec = buf.nextLong ();
            ttl_sec = buf.nextInt ();
            byte [] guid_bytes = new byte [21];
            buf.nextBytes (guid_bytes, 1, 20);
            guid = new BigInteger (guid_bytes);
            secret_hash = new byte [20];
            if (onDisk || ((time_usec & 0x8000000000000000L) != 0)) {
                buf.nextBytes (secret_hash, 0, 20);
                time_usec &= 0x7fffffffffffffffL;
            }
            data_hash = new byte [20];
            buf.nextBytes (data_hash, 0, 20);
            put = (buf.nextByte () == 1);
            byte [] client_bytes = new byte [4];
            buf.nextBytes(client_bytes, 0, 4);
            client_id = bytes_to_addr (client_bytes);
        }

        private static final boolean newStyle = false;

        public void serialize (OutputBuffer buf) {
            this.serialize(buf, false);
        }

        public void serialize (OutputBuffer buf, boolean onDisk) {
            if (!onDisk && !Arrays.equals(secret_hash, ZERO_HASH)) 
                buf.add (0x8000000000000000L | time_usec);
            else
                buf.add (time_usec);
            buf.add (ttl_sec);
            // Skip leading zero in guid bytes (due to twos-compliment).
            byte [] guid_bytes = new byte [20];
            StorageManager.guid_to_bytes (guid, guid_bytes, 0);
            buf.add (guid_bytes, 0, 20);
            if (onDisk || !Arrays.equals(secret_hash, ZERO_HASH)) 
                buf.add (secret_hash, 0, 20);
            buf.add (data_hash, 0, 20);
            buf.add ((byte) (put ? 1 : 0));
            buf.add (client_id.getAddress (), 0, 4);
        }

	public void to_byte_buffer (ByteBuffer buf) {
            serialize(new NioOutputBuffer(buf), true);
	}

	public int compareTo (Object rhs) {
	    Key other = (Key) rhs;
            if (time_usec < other.time_usec)
                return -1;
            if (time_usec > other.time_usec)
                return 1;
	    if (ttl_sec < other.ttl_sec)
		return -1;
	    if (ttl_sec > other.ttl_sec)
		return 1;
	    int result;
	    if ((result = guid.compareTo (other.guid)) != 0)
		return result;
	    for (int i = 0; i < secret_hash.length; ++i) {
		if ((result = secret_hash [i] - other.secret_hash [i]) != 0)
		    return result;
	    }
	    for (int i = 0; i < data_hash.length; ++i) {
		if ((result = data_hash [i] - other.data_hash [i]) != 0)
		    return result;
	    }
	    if ((! put) && other.put)
		return -1;
	    if (put && (! other.put))
		return 1;
            byte [] a = client_id.getAddress();
            byte [] b = other.client_id.getAddress ();
            assert a.length == b.length;
            for (int i = 0; i < a.length; ++i) {
                // treat these as unsigned integers
                int ai = ((int) a [i]) & 0xff;
                int bi = ((int) b [i]) & 0xff;
                if ((result = ai - bi) != 0)
                    return result;
            }
	    return 0;
	}

        public boolean equals (Object rhs) {
            return compareTo (rhs) == 0;
        }

        public void toStringBuffer(StringBuffer buf) {
            buf.append ("key=0x");
            buf.append (GuidTools.guid_to_string(guid));
            buf.append (" secret_hash=0x");
            bytes_to_sbuf(secret_hash, 0, 4, buf);
            buf.append (" data_hash=0x");
            bytes_to_sbuf(data_hash, 0, 4, buf);
            buf.append (" time_usec=0x");
            buf.append (Long.toHexString (time_usec));
            buf.append (" ttl=");
            buf.append (ttl_sec);
            buf.append (" client_id=");
            buf.append (client_id.getHostAddress ());
        }

	public String toString () {
            StringBuffer buf = new StringBuffer(100);
            toStringBuffer(buf);
	    return buf.toString();
	}
    }

    /**
     * Put a new datum into the database.
     */
    public static class PutReq implements QueueElementIF {
        public Key key;
        public ByteBuffer data;
        public SinkIF comp_q;
        public Object user_data;

        public PutReq (Key k, ByteBuffer d, SinkIF s, Object ud) {
            key = k; data = d; comp_q = s; user_data = ud;
        }

        public String toString () {
            return "(PutReq key=" + key + " user_data=" + user_data + ")";
        }
    }

    /**
     * The result of a PutReq, if removed_key is non-null, then removed_key
     * and removed_data where made irrelevant by the put and have been
     * removed from the database.
     */
    public static class PutResp implements QueueElementIF {
        public Key inval_put_key;
        public ByteBuffer inval_put_data;
        public Key inval_rm_key;
        public ByteBuffer inval_rm_data;
        public Object user_data;
        public PutResp (
		Key ipk, ByteBuffer ipd, Key irk, ByteBuffer ird, Object ud) {
	    inval_put_key = ipk; inval_put_data = ipd;
	    inval_rm_key = ipk; inval_rm_data = ipd;
	    user_data = ud;
	}

        public String toString () {
            return "(PutResp inval_put_key=" +
		inval_put_key + " inval_put_data=" + inval_put_data +
		" inval_rm_key=" + inval_rm_key + " inval_rm_data=" +
		inval_rm_data + " user_data=" + user_data + ")";
        }
    }

    public static class GetByKeyReq implements QueueElementIF {
        public Key key;
        public SinkIF comp_q;
        public Object user_data;
        public GetByKeyReq (Key k, SinkIF s, Object ud) {
            key = k; comp_q = s; user_data = ud;
        }
        public String toString () {
            return "(GetByKeyReq key=" + key + " comp_q=" + comp_q + ")";
        }
    }

    public static class GetByKeyResp implements QueueElementIF {
        public Key key;
        public ByteBuffer data;
        public Object user_data;
        public GetByKeyResp (Key k, ByteBuffer d, Object ud) {
            key = k; data = d; user_data = ud;
        }
        public String toString () {
            return "(GetByKeyResp key=" + key + " data=" +
                (data == null ? "null" : "<data>") + ")";
        }
    }

    /**
     * Get all the data in the database whose keys contain the given guid.
     * Think of this a creating an iterator on a selection operator, and
     * calling next once.  If <code>primary</code> is true, data are read from
     * the primary database; otherwise, they are read from the recycling bin.
     */
    public static class GetByGuidReq implements QueueElementIF {
        public BigInteger guid;
        public boolean primary;
        public Key placemark;
        public SinkIF comp_q;
        public Object user_data;
        public GetByGuidReq (
                BigInteger g, boolean p, Key pl, SinkIF s, Object ud) {
            guid = g; primary = p; placemark = pl; comp_q = s; user_data = ud;
        }
        public String toString () {
            return "(GetByGuidReq guid=" + GuidTools.guid_to_string (guid) +
                " primary=" + primary + " placemark=" + placemark +
                " comp_q=" + comp_q + ")";
        }
    }

    /**
     * The (possibly) partial result of a get.  If the continuation is
     * non-null, it may be sent out in a GetByGuidCont request to get the
     * next matching datum, if any.  If the continuation is null, all
     * matching data have been returned.
     */
    public static class GetByGuidResp implements QueueElementIF {
        public Key key;
        public ByteBuffer data;
        public Object continuation;
        public Object user_data;
        public GetByGuidResp (Key k, ByteBuffer d, Object c, Object ud) {
            key = k; data = d; continuation = c; user_data = ud;
        }
        public String toString () {
            return "(GetByGuidResp key=" + key + " data length=" 
                + (data == null ? 0 : data.limit ()) + ")";
        }
    }

    /**
     * Continue an existing GetByGuidReq.  Set continuation to the
     * continuation in the last GetByGuidResp.  To close the cursor associated
     * with a GetByGuidReq, send one of these with a null comp_q.  To delete
     * the last item returned by a request, set del to true.
     */
    public static class GetByGuidCont implements QueueElementIF {
        public Object continuation;
        public boolean del;
        public SinkIF comp_q;
        public Object user_data;
        public GetByGuidCont (Object c, boolean d, SinkIF s, Object ud) {
            continuation = c; del = d; comp_q = s; user_data = ud;
        }
        public GetByGuidCont (Object c, SinkIF s, Object ud) {
            this (c, false, s, ud);
        }
        public String toString () {
            return "(GetByGuidCont continuation=" + continuation +
                " del=" + del + " comp_q=" + comp_q + ")";
        }
    }

    /**
     * Get all the data in the database whose keys have timestamps in the
     * range [low, high].  Think of this a creating an iterator on a
     * selection operator, and calling next once.
     */
    public static class GetByTimeReq implements QueueElementIF {
	public long low, high;
	public SinkIF comp_q;
        public Object user_data;
	public GetByTimeReq (long l, long h, SinkIF s, Object ud) {
	    low = l; high = h; comp_q = s; user_data = ud;
	}
	public String toString () {
	    return "(GetByTimeReq low=" + Long.toHexString (low) + " high=" +
                Long.toHexString (high) + " comp_q=" + comp_q + ")";
	}
    }

    /**
     * The (possibly) partial result of a get.  If the continuation is
     * non-null, it may be sent out in a GetByTimeCont request to get the
     * next matching datum, if any.  If the continuation is null, all
     * matching data have been returned.
     */
    public static class GetByTimeResp implements QueueElementIF {
        public LinkedList<Key> keys;
        public Object continuation;
        public Object user_data;
        public GetByTimeResp (LinkedList<Key> k, Object c, Object ud) {
            keys = k; continuation = c; user_data = ud;
        }
        public String toString () {
            return "(GetByTimeResp keys.size=" + 
                (keys == null ? 0 : keys.size ()) + ")";
        }
    }

    /**
     * Continue an existing GetByTimeReq.  Set continuation to the
     * continuation in the last GetByTimeResp.  To close the cursor associated
     * with a GetByTimeReq, send one of these with a null comp_q.
     */
    public static class GetByTimeCont implements QueueElementIF {
        public Object continuation;
        public SinkIF comp_q;
        public Object user_data;
        public GetByTimeCont (Object c, SinkIF s, Object ud) {
            continuation = c; comp_q = s; user_data = ud;
        }
        public String toString () {
            return "(GetByTimeCont continuation=" + continuation +
                " comp_q=" + comp_q + ")";
        }
    }

    /**
     * Drop a datum from the primary database; optionally move to the
     * recycling bin.
     */
    public static class DiscardReq implements QueueElementIF {
	public Key key;
	public boolean recycle;
	public DiscardReq (Key k, boolean r) {
	    key = k; recycle = r;
	}
	public String toString () {
	    return "(DiscardReq key=" + key + " recycle=" + recycle + ")";
	}
    }

    /**
     * Get all the data in the database whose keys have guids in the
     * range [low, high].  Think of this a creating an iterator on a
     * selection operator, and calling next once.
     */
    public static class IterateByGuidReq implements QueueElementIF {
	public BigInteger low, high;
	public SinkIF comp_q;
        public Object user_data;
	public IterateByGuidReq (BigInteger l, BigInteger h,
				 SinkIF s, Object ud) {
	    low = l; high = h; comp_q = s; user_data = ud;
	}
	public String toString () {
	    return "(IterateByGuidReq low=" + GuidTools.guid_to_string (low) +
		" high=" + GuidTools.guid_to_string (high) +
                " comp_q=" + comp_q + ")";
	}
    }

    /**
     * If the continuation is non-null, it may be sent out in a
     * IterateByGuidCont request to get the next datum, if any.  If the
     * continuation is null, all data have been returned.
     */
    public static class IterateByGuidResp implements QueueElementIF {
        public StorageManager.Key key;
        public ByteBuffer data;
        public Object continuation;
        public Object user_data;
        public IterateByGuidResp (StorageManager.Key k, ByteBuffer d, Object c, Object ud) {
            key = k; data = d; continuation = c; user_data = ud;
        }
        public String toString () {
            return "(IterateByGuidResp key=" + key + ")";
        }
    }

    /**
     * Continue an existing IterateByGuidReq.  Set continuation to the
     * continuation in the last IterateByGuidResp.  To close the cursor
     * associated with a IterateByGuidReq, send one of these with a null
     * comp_q.
     */
    public static class IterateByGuidCont implements QueueElementIF {
        public Object continuation;
        public SinkIF comp_q;
        public Object user_data;
        public IterateByGuidCont (Object c, SinkIF s, Object ud) {
            continuation = c; comp_q = s; user_data = ud;
        }
        public String toString () {
            return "(IterateByGuidCont continuation=" + continuation +
                " comp_q=" + comp_q + ")";
        }
    }

    protected static class IBGCont {
	public Dbt key;
	public BigInteger low, high;
	public IBGCont (Dbt k, BigInteger l, BigInteger h) {
	    key = k; low = l; high = h;
	}
    }

    protected static class GBGCont {
        public BigInteger guid;
        public Dbt key;
        public boolean primary;
        public Key placemark;
        public GBGCont (BigInteger g, Dbt k, boolean p, Key pl) {
            guid = g; key = k; primary = p; placemark = pl;
        }
    }

    protected static class GBTCont {
	public Dbt key;
	public long low, high;
	public GBTCont (Dbt k, long l, long h) {
	    key = k; low = l; high = h;
	}
    }

    protected static class Alarm implements QueueElementIF {}
    protected static class SyncAlarm implements QueueElementIF {}

    protected class ShutdownHook extends Thread {
        public void run() {
            logger.info ("shutdown hook checkpointing database");
            try { 
                env.txnCheckpoint(0, 0, Db.DB_FORCE); 
            }
            catch (DbException e) { 
                logger.fatal("caught exception", e); 
                return;
            }
            logger.info ("shutdown hook checkpoint complete");
        }
    }

    protected Db client_counts;
    protected Db by_time;
    protected Db by_guid;
    protected Db by_guid_and_data_hash;
    protected Db recycling;
    protected DbEnv env;
    protected LinkedList to_bdb_thread = new LinkedList ();
    protected Map open_cursors = new HashMap ();

    public StorageManager () {
        event_types = new Class [] {
            Alarm.class,
            SyncAlarm.class,
            EnqueueEvent.class,
            PutReq.class,
            GetByKeyReq.class,
            GetByGuidReq.class,
            GetByGuidCont.class,
            GetByTimeReq.class,
            GetByTimeCont.class,
	    IterateByGuidReq.class,
	    IterateByGuidCont.class,
            DiscardReq.class
        };
    }

    protected DbSecondaryKeyCreate guid_key_creator = 
        new DbSecondaryKeyCreate() {
        public int secondaryKeyCreate(Db secdb, Dbt key, Dbt data,
                                      Dbt result) {
            byte [] buf = new byte [key.getSize ()];
            System.arraycopy (key.getData (), key.getOffset (),
                              buf, 0, buf.length);
            primary_key_to_recycling_key (buf);
            result.setData (buf);
            result.setOffset (0);
            result.setSize (buf.length);
            return 0;
        }
    };

    DbSecondaryKeyCreate guid_and_data_hash_key_creator =
        new DbSecondaryKeyCreate () {
            public int secondaryKeyCreate(Db secdb, Dbt key, Dbt data,
                                          Dbt result) {
                result.setData (key.getData ());
                result.setOffset (key.getOffset () + 12);
                result.setSize (60);
                return 0;
            }
        };

    protected Random rand;

    public void init (ConfigDataIF config) throws Exception {

        super.init (config);

	// Initialize tables

        String homedir = config_get_string (config, "homedir");
        File directory = new File(homedir);

        // Create the directory if it doesn't exist.
        try {
            if (! directory.exists ()) {
                if (! directory.mkdirs ()) {
                    logger.fatal ("could not mkdir " + homedir);
                    System.exit (1);
                }
            }
        }
        catch (SecurityException e) {
            logger.fatal ("caught " + e + " trying to create " + homedir, e);
        }
	boolean print_open_cursors = 
	    config_get_boolean(config, "print_open_cursors");
        int cache_size = config_get_int(config, "libdb_cache_size");
        if (cache_size == -1)
            cache_size = 1024 * 1024;

        // Open the environment.
        env = new DbEnv(0);
        env.setCacheSize(cache_size, 1);
        env.setFlags(Db.DB_TXN_NOSYNC, true);
        env.setFlags(Db.DB_LOG_AUTOREMOVE, true);
        env.open(homedir, Db.DB_INIT_MPOOL | Db.DB_INIT_TXN | Db.DB_INIT_LOCK | 
                Db.DB_RECOVER | Db.DB_CREATE, 0);

        // Open the client counts DB.

        DbTxn txn = env.txnBegin(null, Db.DB_TXN_SYNC);
        client_counts = new Db(env, 0);
        client_counts.open(txn, "client_counts.db", null, Db.DB_BTREE,
                Db.DB_CREATE | Db.DB_DIRTY_READ, 0);
        txn.commit(Db.DB_TXN_SYNC);

        // Open the primary DB.

        txn = env.txnBegin(null, Db.DB_TXN_SYNC);
        by_time = new Db(env, 0);
        by_time.open(txn, homedir+"/by_time.db", null, Db.DB_BTREE,
                     Db.DB_CREATE | Db.DB_DIRTY_READ, 0);

        // Open the by-guid index.

        by_guid = new Db(env, 0);
        by_guid.setFlags(Db.DB_DUPSORT);
        by_guid.open(txn, homedir+"/by_guid.db", null, Db.DB_BTREE,
                     Db.DB_CREATE | Db.DB_DIRTY_READ, 0);
        by_time.associate(txn, by_guid, guid_key_creator, 0);

        // Open the by-guid-and-data-hash index.

        by_guid_and_data_hash = new Db(env, 0);
        by_guid_and_data_hash.setFlags(Db.DB_DUPSORT);
        by_guid_and_data_hash.open(txn, homedir+"/by_guid_and_data_hash.db",
                                   null, Db.DB_BTREE,
                                   Db.DB_CREATE | Db.DB_DIRTY_READ, 0);
        by_time.associate(txn, by_guid_and_data_hash,
                          guid_and_data_hash_key_creator, 0);


        txn.commit(Db.DB_TXN_SYNC);

        // Open the recycling bin.

        txn = env.txnBegin(null, Db.DB_TXN_SYNC);
        recycling = new Db(env, 0);
        recycling.open(txn, homedir+"/recycling.db", null, Db.DB_BTREE,
                       Db.DB_CREATE | Db.DB_DIRTY_READ, 0);
        txn.commit(Db.DB_TXN_SYNC);

        // Clean the log.
        /*
        logger.info ("Cleaning log");
        try { env.cleanLog (); }
        catch (DbException e) { BUG (e); }
        */

        // Do a test read.
        
        long test_start = now_ms ();
        logger.info ("Testing database");
        Key k = ZERO_KEY;
        byte [] kbuf = new byte [Key.SIZE];
        ByteBuffer bb = ByteBuffer.wrap (kbuf);
        k.to_byte_buffer (bb);
        Dbt key = new Dbt (kbuf);
        key.setSize (kbuf.length);
        Dbc cursor = open_cursor (by_time, null, "handle_get_by_time_req");
        int retval = 0;
        Dbt data = new Dbt ();
        data.setFlags(Db.DB_DBT_PARTIAL);
        data.setPartialLength(0);
        try { retval = cursor.get (key, data, Db.DB_SET_RANGE); }
        catch (DbException e) { BUG(e); }
        close_cursor (cursor);
        long test_end = now_ms ();
        if (test_end - test_start < 60*1000) {
            logger.info ("Test successful");
        }
        else {
            logger.fatal ("Test took too long.  Reading one value took " +
                    ((test_end - test_start) / 1000.0) + " seconds");

            logger.fatal ("Closing environment");

            try {
                txn = env.txnBegin(null, Db.DB_TXN_SYNC);
                env.close (0);
                txn.commit(Db.DB_TXN_SYNC);
            }
            catch (DbException e) { BUG (e); }

            logger.fatal ("Exiting");

            System.exit (1);
        }
 
        // Start the BerekelyDB thread.

        if (! sim_running) {
            Thread t = new Thread () {
                public void run () {
                    try {
                        while (true) {
                            QueueElementIF head = null;
                            int sz = 0;
                            synchronized (to_bdb_thread) {
                                while (to_bdb_thread.isEmpty()) {
                                    try { to_bdb_thread.wait(); }
                                    catch (InterruptedException e) {}
                                }
                                head = (QueueElementIF)
                                    to_bdb_thread.removeFirst();
                                sz = to_bdb_thread.size ();
                                last_dequeue_ms = timer_ms ();
                            }
                            if (logger.isDebugEnabled ())
                                logger.debug ("to_bdb_thread.size()=" + sz);
                            the_real_handle_event(head);
                        }
                    }
                    catch (OutOfMemoryError e) {
                        bamboo.lss.DustDevilSink.reserve = null;
                        System.gc ();
                        logger.fatal ("uncaught error", e);
                        System.exit (1);
                    }
                    catch (Throwable e) {
                        logger.fatal ("uncaught exception", e);
                        System.exit (1);
                    }
                }
            };
            t.setName ("StorageManager.BerkeleyDbThread-"+my_node_id);
            t.start ();
        }

        // Setup the shutdown hook.

        Runtime.getRuntime().addShutdownHook(new ShutdownHook());

        rand = new Random (now_ms () ^ my_node_id.hashCode ());
        classifier.dispatch_later (new SyncAlarm (), 
                30*1000 + rand.nextInt (60*1000));
        classifier.dispatch_later (new Alarm (), 
                30*1000 + rand.nextInt (60*1000));
    }

    // For the watchdog timer.
    public long last_dequeue_ms;

    public void handleEvent (QueueElementIF item) {
        if (item instanceof EnqueueEvent) {
            EnqueueEvent ee = (EnqueueEvent) item;
            try {
                ee.sink.enqueue (ee.item);
            }
            catch (SinkException e) {
                BUG ("couldn't enqueue " + e);
            }
        }
        else {
            if (logger.isDebugEnabled ()) logger.debug ("got " + item);
            if (sim_running) {
                the_real_handle_event (item);
            }
            else {
                long now_ms = 0;
                long last_ms = 0;
                int count = 0;
                synchronized (to_bdb_thread) {
                    to_bdb_thread.addLast (item);
                    to_bdb_thread.notifyAll ();
                    count = to_bdb_thread.size ();
                    now_ms = timer_ms ();
                    if (count == 1)
                        last_dequeue_ms = now_ms;
                    last_ms = last_dequeue_ms;
                }

                if (count > 0) { 
                    if (now_ms - last_ms > 180*1000) {
                        logger.fatal ("BerkeleyDB thread " 
				+ Thread.currentThread().getName()
				+ " blocked. "
                                + " now_ms=" + now_ms
                                + " last_dequeue_ms=" + last_ms
                                + ".  There are at least " + count
                                + " outstanding requests.  " 
                                + "Committing suicide.");
                        System.exit (1);
                    }
                    if (now_ms - last_ms > 60*1000) {
                        logger.warn ("BerkeleyDB thread " 
				+ Thread.currentThread().getName()
				+ " appears blocked. "
                                + " now_ms=" + now_ms
                                + " last_dequeue_ms=" + last_ms
                                + ".  There are at least " + count
                                + " outstanding requests.");
                    }
                }
            }
        }
    }

    /**
     * This function is only ever called from within the Berkeley DB thread
     * launched from the init () function; as such, it can safely block.  To
     * communicate with the main thread, this thread calls
     * classifier.dispatch_later with a time of 0.
     */
    protected void the_real_handle_event (QueueElementIF item) {
        if (logger.isDebugEnabled ()) logger.debug ("handling " + item);

        if (item instanceof PutReq) {
            handle_put_req ((PutReq) item);
        }
        else if (item instanceof GetByKeyReq) {
            handle_get_by_key_req ((GetByKeyReq) item);
        }
        else if (item instanceof GetByGuidReq) {
            handle_get_by_guid_req ((GetByGuidReq) item);
        }
        else if (item instanceof GetByGuidCont) {
            handle_get_by_guid_cont ((GetByGuidCont) item);
        }
        else if (item instanceof GetByTimeReq) {
            handle_get_by_time_req ((GetByTimeReq) item);
        }
        else if (item instanceof GetByTimeCont) {
            handle_get_by_time_cont ((GetByTimeCont) item);
        }
        else if (item instanceof IterateByGuidReq) {
            handle_iterate_by_guid_req ((IterateByGuidReq) item);
        }
        else if (item instanceof IterateByGuidCont) {
            handle_iterate_by_guid_cont ((IterateByGuidCont) item);
        }
        else if (item instanceof DiscardReq) {
            handle_discard_req ((DiscardReq) item);
        }
        else if (item instanceof SyncAlarm) {
            logger.info ("starting checkpoint");
            // Checkpoint after every 30 MBs of log data.
            try { env.txnCheckpoint (30*1024, 0, 0); }
            catch (DbException e) { BUG (e); }
            logger.info ("checkpoint done");
            classifier.dispatch_later (item, 30*1000 + rand.nextInt (60*1000));
        }
        else if (item instanceof Alarm) {
            check_open_cursors ();
            classifier.dispatch_later (item, 30*1000 + rand.nextInt (60*1000));
        }
        else if (item instanceof AddMonitor) {
            handle_add_monitor ((AddMonitor) item);
        }
        else if (item instanceof RemoveMonitor) {
            handle_remove_monitor ((RemoveMonitor) item);
        }
        else {
            assert false : item.getClass ().getName ();
        }
    }

    protected void check_open_cursors () { 
	if(print_open_cursors)
	    logger.info (open_cursors.size () + " open cursors");
    }

    protected Dbc open_cursor (Db db, DbTxn xact, String source) {
        assert source != null;
        Long open_time_ms = new Long (timer_ms ());
        Dbc cursor = null;
        try { cursor = db.cursor(xact, (xact == null ? 0 : Db.DB_DIRTY_READ)); }
        catch (DbException e) { BUG (e); }
        open_cursors.put (cursor, new Object [] {source, open_time_ms});
        return cursor;
    }

    protected void close_cursor (Dbc cursor) {
        Object removed = open_cursors.remove (cursor);
        assert removed != null;
        try { cursor.close (); }
        catch (DbException e) { BUG (e); }
    }

    protected void handle_put_req (PutReq req) {

        // There should be at most one put or one remove stored in the
        // database for this (guid, secret_hash, data_hash) triple.  We need
        // to check for them first, then put this new datum in, and then
        // return whichever existing datum it overrides, if any.  Also, we
        // have to transaction-protect the whole sequence in order to maintain
        // the property that there is at most one put or one remove each in
        // the database.  We do specify DB_TXN_NOSYNC, however, so that we get
        // atomicity, consistency, and isolation, but not necessarily
        // durability (in return for increased performance).  This tradeoff is
        // safe since if the tuple is lost, it will be recovered by the
        // epidemic algorithms in Bamboo.

        DbTxn xact = null;

        if (logger.isDebugEnabled())
            logger.debug("doing put, key=" + req.key);
        try { xact = env.txnBegin(null, Db.DB_TXN_NOSYNC); }
        catch (DbException e) { BUG(e); }

        Key inval_put_key = null;
        ByteBuffer inval_put_data = null;
        Key inval_rm_key = null;
        ByteBuffer inval_rm_data = null;

        Dbc cursor = open_cursor(by_guid_and_data_hash, xact, "handle_put_req");
        Dbt skey = null;
        {
            byte[] buf = new byte[60];
            guid_to_bytes(req.key.guid, buf, 0);
            System.arraycopy(req.key.secret_hash, 0, buf, 20, 20);
            System.arraycopy(req.key.data_hash, 0, buf, 40, 20);
            skey = new Dbt(buf);
            skey.setSize(buf.length);
        }

        boolean do_put = true;
        Dbt pkey = new Dbt ();
        Dbt data = new Dbt ();
        int retval = 0;
        try { retval = cursor.get(skey, pkey, data, Db.DB_SET); }
        catch (DbException e) { BUG(e); }

        if (retval == Db.DB_NOTFOUND) {
            if (logger.isDebugEnabled ())
                logger.debug ("no existing keys with same guid and data hash");
        }
        else {

            assert retval == 0 : retval;

            Key k = new Key(pkey);
            if (logger.isDebugEnabled ())
                logger.debug ("existing tuple key=" + k);
            assert k.guid.equals (req.key.guid);
            assert Arrays.equals (req.key.data_hash, k.data_hash);

            if (req.key.compareTo (k) == 0) {
                // They're the same.
                do_put = false;
            }
            else {
                if (req.key.put && k.put) {
                    if (req.key.expiryTime() <= k.expiryTime()) {
                        // This put will expire before the existing put, so
                        // just throw it away.
                        do_put = false;
                        inval_put_key = req.key;
                        inval_put_data = req.data;
                    }
                    else {
                        // Remove the existing put.  Because there's an
                        // existing put, there must not be an existing remove,
                        // so we can break.
                        inval_put_key = k;
                        inval_put_data = ByteBuffer.wrap (data.getData (),
                                                          data.getOffset (),
                                                          data.getSize ());
                        try { cursor.delete (0); }
                        catch (DbException e) { BUG (e); }
                        storage_changed (false /* removed */,
                                         inval_put_key, data.getSize(), xact);
                    }
                }
                else if (req.key.put && (! k.put)) {
                    // We have an existing remove.  Lose the new put.
                    do_put = false;
                    inval_put_key = req.key;
                    inval_put_data = req.data;
                }
                else if ((! req.key.put) && (! k.put)) {
                    if (req.key.expiryTime() <= k.expiryTime()) {
                        // This remove will expire before the existing remove.
                        do_put = false;
                        inval_rm_key = req.key;
                        inval_rm_data = req.data;
                    }
                    else {
                        // Replace the existing remove.  Because there's an
                        // existing remove, there shouldn't be any existing
                        // put, so we can break;
                        inval_rm_key = k;
                        inval_rm_data = ByteBuffer.wrap (data.getData (),
                                                         data.getOffset (),
                                                         data.getSize ());
                        try { cursor.delete (0); }
                        catch (DbException e) { BUG (e); }
                        storage_changed (false /* removed */,
                                         inval_rm_key, data.getSize(), xact);
                    }
                }
                else if ((! req.key.put) && k.put) {
                    // This remove invalidates the existing put.  Because
                    // there's an exising put, there must not be an existing
                    // remove, and we can break.

                    inval_put_key = k;
                    inval_put_data = ByteBuffer.wrap(
                            data.getData(), data.getOffset(), data.getSize());
                    try { cursor.delete(0); }
                    catch (DbException e) { BUG(e); }
                    storage_changed(false /* removed */,
                                    inval_put_key, data.getSize(), xact);
                }
            } // not the same key

            try { retval = cursor.get(skey, pkey, data, Db.DB_NEXT_DUP); }
            catch (DbException e) { BUG(e); }

            assert retval == Db.DB_NOTFOUND;
	} // if found

        close_cursor (cursor);

        // Should have been at most one thing removed.

        assert ! ((inval_put_key != null) && (inval_rm_key != null));

	// At this point, we removed any data that this new datum should
	// invalidate.  Now we do the put if it's still necessary.

        if (do_put) {
            ByteBuffer kbuf = ByteBuffer.allocate (Key.SIZE);
            req.key.to_byte_buffer (kbuf);
            Dbt key = new Dbt (kbuf.array (),
                                                   kbuf.arrayOffset (),
                                                   kbuf.limit ());
            key.setSize (kbuf.limit ());

            if (req.data.hasArray ()) {
                data = new Dbt (req.data.array (),
                                          req.data.arrayOffset (),
                                          req.data.limit ());
            }
            else {
                byte [] ary = new byte [req.data.limit ()];
                req.data.get (ary, 0, ary.length);
                data = new Dbt (ary);
            }
            data.setSize (req.data.limit ());

            try { retval = by_time.put (xact, key, data, 0); }
            catch (DbException e) { BUG (e); }

            storage_changed (true /* added */, req.key, data.getSize(), xact);
            assert retval == 0;

            if (logger.isInfoEnabled ()) {
                int len = 100
                    + ((inval_put_key == null) ? 0 : 50)
                    + ((inval_rm_key == null) ? 0 : 50);
                StringBuffer buf = new StringBuffer (len);
                if (req.key.put)
                    buf.append ("put key=0x");
                else
                    buf.append ("rem key=0x");
                buf.append (GuidTools.guid_to_string (req.key.guid));
                buf.append (" time_usec=0x");
                buf.append (Long.toHexString (req.key.time_usec));
                buf.append (" ttl=");
                buf.append (req.key.ttl_sec);
                buf.append (" client_id=");
                buf.append (req.key.client_id.getHostAddress ());
                buf.append (" secret_hash=0x");
                buf.append (bytes_to_str(req.key.secret_hash, 0, 4));
                buf.append (" data_hash=0x");
                buf.append (bytes_to_str(req.key.data_hash, 0, 4));
                buf.append (" size=");
                buf.append (data.getSize ());
                if (inval_put_key != null) {
                    buf.append (" overwrites old put secret_hash=0x");
                    buf.append (bytes_to_str(inval_put_key.secret_hash, 0, 4));
                    buf.append (" data_hash=0x");
                    buf.append (bytes_to_str(inval_put_key.data_hash, 0, 4));
                }
                else if (inval_rm_key != null) {
                    buf.append (" overwrites old rem secret_hash=0x");
                    buf.append (bytes_to_str(inval_rm_key.secret_hash, 0, 4));
                    buf.append (" data_hash=0x");
                    buf.append (bytes_to_str(inval_rm_key.data_hash, 0, 4));
                }
                logger.info (buf);
            }
        }

	try { xact.commit(Db.DB_TXN_NOSYNC); }
	catch (DbException e) { BUG (e); }

        PutResp resp = new PutResp (inval_put_key, inval_put_data,
		inval_rm_key, inval_rm_data, req.user_data);

        application_enqueue (req.comp_q, resp);
    }

    protected boolean key_expired(Key k) {
	return k.expiryTime() < ((long) now_ms() * 1000);
    }

    protected void handle_get_by_key_req (GetByKeyReq req) {
        byte [] buf = new byte [Key.SIZE];
        req.key.to_byte_buffer (ByteBuffer.wrap (buf));
        Dbt key = new Dbt (buf);
        key.setSize (buf.length);
        Dbt data = new Dbt ();

	// key itself tells us if expired; expire w/o fetching if so
	if (key_expired(req.key)) {
	    if (logger.isDebugEnabled ())
                logger.debug("dropping in handle_get_by_key_req");
	    the_real_handle_event (new DiscardReq(req.key, false));
	    application_enqueue(req.comp_q,
                                new GetByKeyResp (req.key, (ByteBuffer) null,
                                                  req.user_data));
	    return;
	}

	int retval = 0;
	try { retval = by_time.get (null, key, data, 0); }
	catch (DbException e) { BUG (e); }

	ByteBuffer dbuf = null;
	if (retval == 0) {
	    dbuf = ByteBuffer.wrap (data.getData (), data.getOffset (),
				    data.getSize ());
	}
	else {
            assert retval == Db.DB_NOTFOUND : retval;
        }

        application_enqueue (req.comp_q,
                             new GetByKeyResp (req.key, dbuf, req.user_data));
    }

    protected void primary_key_to_recycling_key (byte [] buf) {
        byte [] tmp = new byte [20];
        System.arraycopy (buf, 12, tmp, 0, 20); // back up guid
        System.arraycopy (buf, 0, buf, 20, 8); // move timestamp
        System.arraycopy (buf, 8, buf, 28, 4); // move ttl
        System.arraycopy (tmp, 0, buf, 0, 20); // restore guid
    }

    protected void handle_get_by_guid_req (GetByGuidReq req) {
        Key k = req.placemark;
        if ((k == null) || (k.equals (ZERO_KEY))) {
            // If no placemark, start at the beginning.
            k = new Key(0L, 0, req.guid, ZERO_HASH, ZERO_HASH, false, 
                        ZERO_CLIENT);
        }

        byte [] buf = new byte [Key.SIZE];
        k.to_byte_buffer (ByteBuffer.wrap (buf));
        primary_key_to_recycling_key (buf);
        Dbt key = new Dbt (buf);
        key.setSize (buf.length);

        if (req.primary) {
            DbTxn xact = null;
            try { xact = env.txnBegin(null, Db.DB_TXN_NOSYNC); }
            catch (DbException e) { BUG(e); }
            Dbc cursor = open_cursor(by_guid, xact, "handle_get_by_guid_req");
            Dbt pkey = new Dbt ();
            Dbt data = new Dbt ();
            int r = 0;
            try { r = cursor.get(key, pkey, data, Db.DB_SET_RANGE); }
            catch (DbException e) { BUG(e); }

            GBGCont cont = new GBGCont(req.guid, key, true, req.placemark);
            while (! finish_get_by_guid (cont, xact, cursor, pkey, data, r, 
                                         req.comp_q, req.user_data)) {
                try { r = cursor.get(cont.key, pkey, data, Db.DB_NEXT);}
                catch (DbException e) { BUG(e); }
            }
            close_cursor (cursor);
            try { xact.commit(Db.DB_TXN_NOSYNC); }
            catch (DbException e) { BUG(e); }
        }
        else {
            DbTxn xact = null;
            try { xact = env.txnBegin(null, Db.DB_TXN_NOSYNC); }
            catch (DbException e) { BUG(e); }
            Dbc cursor = open_cursor(recycling, xact, "handle_get_by_guid_req");
            Dbt data = new Dbt ();
            int r = 0;
            try { r = cursor.get(key, data, Db.DB_SET_RANGE); }
            catch (DbException e) { BUG(e); }

            GBGCont cont = new GBGCont(req.guid, key, false, req.placemark);
            while (! finish_get_by_guid_recycling (cont, xact, cursor, data, r, 
                                                   req.comp_q, req.user_data)) {
                try { r = cursor.get(cont.key, data, Db.DB_NEXT);}
                catch (DbException e) { BUG(e); }
            }
            close_cursor (cursor);
            try { xact.commit(Db.DB_TXN_NOSYNC); }
            catch (DbException e) { BUG(e); }
        }
    }

    protected void handle_get_by_guid_cont (GetByGuidCont req) {
        GBGCont cont = (GBGCont) req.continuation;

        if (! cont.primary) {
            DbTxn xact = null;
            try { xact = env.txnBegin(null, Db.DB_TXN_NOSYNC); }
            catch (DbException e) { BUG(e); }
                
            Dbc cursor = open_cursor (recycling, xact, 
                                         "handle_get_by_guid_req");
            Dbt data = new Dbt ();
            data.setFlags(Db.DB_DBT_PARTIAL);
            data.setPartialLength(0);
            byte [] last_key = new byte [cont.key.getSize ()];
            System.arraycopy (cont.key.getData (), cont.key.getOffset (), 
                              last_key, 0, cont.key.getSize ());
            int r = 0;
            try { r = cursor.get (cont.key, data, Db.DB_SET_RANGE); }
            catch (DbException e) { BUG(e); }
            if (r == 0) {
                int cmp = arraycmp(cont.key.getData(), cont.key.getOffset(),
                                   last_key, 0, cont.key.getSize ());
                assert cmp >= 0;
                if (cmp == 0) {
                    if (req.del) {
                        try { cursor.delete (0); }
                        catch (DbException e) { BUG(e); }
                    }
                    data = new Dbt (); // don't setPartial
                    try { r = cursor.get (cont.key, data, Db.DB_NEXT); }
                    catch (DbException e) { BUG(e); }
                }
                else {
                    data = new Dbt (); // don't setPartial
                    try { r = cursor.get (cont.key, data, Db.DB_CURRENT); }
                    catch (DbException e) { BUG(e); }
                }
            }

            if (req.comp_q != null) {
                while(!finish_get_by_guid_recycling(cont, xact,cursor, data, r, 
                                                    req.comp_q,req.user_data)) {
                    try { r = cursor.get (cont.key, data, Db.DB_NEXT); }
                    catch (DbException e) { BUG(e); }
                }
            }
            close_cursor (cursor);
            try { xact.commit(Db.DB_TXN_NOSYNC); }
            catch (DbException e) { BUG(e); }
        }
        else if (req.comp_q != null) {
            DbTxn xact = null;
            try { xact = env.txnBegin(null, Db.DB_TXN_NOSYNC); }
            catch (DbException e) { BUG(e); }
            Dbc sc = open_cursor(by_guid, xact, "handle_get_by_guid_req");
            Dbt pkey = new Dbt ();
            Dbt data = new Dbt ();
            data.setFlags(Db.DB_DBT_PARTIAL);
            data.setPartialLength(0);
            byte [] last_key = new byte [cont.key.getSize ()];
            System.arraycopy(cont.key.getData (), cont.key.getOffset (), 
                    last_key, 0, cont.key.getSize ());

            int r = 0;
            // Search to the last key returned.

            try { r = sc.get (cont.key, pkey, data, Db.DB_SET_RANGE); }
            catch (DbException e) { BUG(e); }

            // If we find it, get the next one.  If we find one that's
            // already greater than the last one (presumably because the
            // last one was removed from the DB), just go with that.
            if (r == 0) {
                int cmp = arraycmp(cont.key.getData(), cont.key.getOffset(),
                                   last_key, 0, cont.key.getSize ());
                assert cmp >= 0;
                if (cmp == 0) {
                    data = new Dbt (); // don't setPartial
                    try { r = sc.get (cont.key, pkey, data, Db.DB_NEXT); }
                    catch (DbException e) { BUG(e); }
                }
                else {
                    data = new Dbt (); // don't setPartial
                    try { r = sc.get (cont.key, pkey, data, Db.DB_CURRENT); }
                    catch (DbException e) { BUG(e); }
                }
            }

            while (! finish_get_by_guid (cont, xact, sc, pkey, data, r,
                                         req.comp_q, req.user_data)) {
                try { r = sc.get(cont.key, pkey, data, Db.DB_NEXT); }
                catch (DbException e) { BUG(e); }
            }
            close_cursor (sc);
            try { xact.commit(Db.DB_TXN_NOSYNC); }
            catch (DbException e) { BUG(e); }
        }
    }

    protected void drop_expired_data (DbTxn xact, Dbc cursor, Key k, int size) {
        try { cursor.delete (0); } 
        catch (DbException e) { BUG(e); }

        storage_changed(false /* removed */, k, size, xact);

        if (logger.isInfoEnabled ()) {
            StringBuffer sbuf = new StringBuffer (105);
            sbuf.append ("dropping expired key=0x");
            sbuf.append (GuidTools.guid_to_string (k.guid));
            sbuf.append (" time_usec=0x");
            sbuf.append (Long.toHexString (k.time_usec));
            sbuf.append (" ttl=");
            sbuf.append (k.ttl_sec);
            sbuf.append (" client_id=");
            sbuf.append (k.client_id.getHostAddress ());
            sbuf.append (" data_hash=0x");
            sbuf.append (bytes_to_str(k.data_hash, 0, 4));
            sbuf.append (" size=");
            sbuf.append (size);
            logger.info (sbuf);
        }
    }

    protected boolean finish_get_by_guid(GBGCont cont, DbTxn xact, Dbc cursor,
                                         Dbt pkey, Dbt data, int retval,
                                         SinkIF comp_q, Object user_data) {

        if (retval == Db.DB_NOTFOUND) {
            if (logger.isDebugEnabled ()) logger.debug ("DB_NOTFOUND");
            GetByGuidResp resp = new GetByGuidResp(null, null, null, user_data);
            application_enqueue (comp_q, resp);
            return true;
        }

        Key k = new Key(pkey);
        if (key_expired(k)) {
            // drop it, grab next
            if (logger.isDebugEnabled ())
                logger.debug("dropping in do_get_by_guid k=" +k);
            drop_expired_data (xact, cursor, k, data.getSize ());
            // the_real_handle_event (new DiscardReq(k, false));
            return false;
        }

        if ((cont.placemark != null)
            && (k.compareTo (cont.placemark) == 0)) {
            // skip it
            if (logger.isDebugEnabled ())
                logger.debug ("first key equal to placemark.  Skipping it.");
            return false;
        }

        if (! k.guid.equals (cont.guid)) {
            if (logger.isDebugEnabled ())
                logger.debug ("wanted guid=" +
                        GuidTools.guid_to_string (cont.guid) +
                        " but got guid=" +
                        GuidTools.guid_to_string (k.guid));
            GetByGuidResp resp =
                new GetByGuidResp (null, null, null, user_data);
            application_enqueue (comp_q, resp);
        }
        else {
            ByteBuffer bb = ByteBuffer.wrap (data.getData (),
                    data.getOffset (), data.getSize ());
            GetByGuidResp resp =
                new GetByGuidResp (k, bb, cont, user_data);
            application_enqueue (comp_q, resp);
        }

        return true;
    }

    protected boolean finish_get_by_guid_recycling (GBGCont cont, DbTxn xact,
                                                    Dbc cursor, Dbt data,
                                                    int retval, SinkIF comp_q,
                                                    Object user_data) {

        if (retval == Db.DB_NOTFOUND) {
            if (logger.isDebugEnabled ()) logger.debug("recycling DB_NOTFOUND");
            GetByGuidResp resp = new GetByGuidResp(null, null, null, user_data);
            application_enqueue (comp_q, resp);
            return true;
        }

        byte [] kbuf = new byte [Key.SIZE];

        System.arraycopy (cont.key.getData (), cont.key.getOffset () + 20,
                          kbuf,  0, 8); // timestamp 1st
        System.arraycopy (cont.key.getData (), cont.key.getOffset () + 28,
                          kbuf, 8, 4);  // then ttl
        System.arraycopy (cont.key.getData (), cont.key.getOffset () + 0,
                          kbuf, 12, 20);  // then guid
        System.arraycopy (cont.key.getData (), cont.key.getOffset () + 32,
                          kbuf, 32, Key.SIZE-32); // rest

        Key k = new Key (new Dbt(kbuf, 0, kbuf.length));
        if (key_expired(k)) {
            // drop it, grab next one
            if (logger.isDebugEnabled ())
                logger.debug ("dropping in do_get_by_guid_recycling");
            try { cursor.delete (0); }
            catch (DbException e) { BUG(e); }
            return false;
        }

        ByteBuffer bb = ByteBuffer.wrap(data.getData (), data.getOffset (),
                                        data.getSize ());
        GetByGuidResp resp = new GetByGuidResp (k, bb, cont, user_data);
        application_enqueue (comp_q, resp);
        return true;
    }

    protected void handle_get_by_time_req (GetByTimeReq req) {
        Key k = new Key(req.low, 0, ZERO_GUID, ZERO_HASH, ZERO_HASH, false, 
                        ZERO_CLIENT);
        byte [] kbuf = new byte [Key.SIZE];
        ByteBuffer bb = ByteBuffer.wrap (kbuf);
        k.to_byte_buffer (bb);
        Dbt key = new Dbt (kbuf);
        key.setSize (kbuf.length);
        DbTxn xact = null;
        try { xact = env.txnBegin(null, Db.DB_TXN_NOSYNC); }
        catch (DbException e) { BUG(e); }
        Dbc cursor = open_cursor (by_time, xact, "handle_get_by_time_req");
        GBTCont cont = new GBTCont (key, req.low, req.high);
        int retval = 0;
        Dbt data = new Dbt ();
        data.setFlags(Db.DB_DBT_PARTIAL);
        data.setPartialLength(0);
        try { retval = cursor.get (key, data, Db.DB_SET_RANGE); }
        catch (DbException e) { BUG(e); }
        finish_get_by_time (cont, xact, cursor, data, retval, 
                            req.comp_q, req.user_data);
    }

    protected int arraycmp (byte [] a1, int o1, byte [] a2, int o2, int sz) {
        for (int i = 0; i < sz; ++i) {
            int l = 0xff & ((int) a1 [o1]);
            int r = 0xff & ((int) a2 [o2]);
            if (l < r)
                return -1;
            else if (l > r)
                return 1;
            ++o1; 
            ++o2;
        }
        return 0;
    }

    protected void handle_get_by_time_cont (GetByTimeCont req) {
        GBTCont cont = (GBTCont) req.continuation;
        if (req.comp_q != null) {
            DbTxn xact = null;
            try { xact = env.txnBegin(null, Db.DB_TXN_NOSYNC); }
            catch (DbException e) { BUG(e); }
            Dbc cursor = open_cursor (by_time, xact, "handle_get_by_time_req");
            int retval = 0;
            Dbt data = new Dbt ();
            data.setFlags(Db.DB_DBT_PARTIAL);
            data.setPartialLength(0);
            // Search to the last key returned.
            byte [] last_key = new byte [cont.key.getSize ()];
            System.arraycopy(cont.key.getData (), cont.key.getOffset (), 
                             last_key, 0, cont.key.getSize ());
            try { retval = cursor.get (cont.key, data, Db.DB_SET_RANGE); }
            catch (DbException e) { BUG(e); }

            // If we find it, get the next one.  If we find one that's already
            // greater than the last one (presumably because the last one was
            // removed from the DB), just go with that.
            if (retval == 0) {
                int cmp = arraycmp (cont.key.getData (), cont.key.getOffset (), 
                                    last_key, 0, cont.key.getSize ());
                assert cmp >= 0;
                if (cmp == 0) {
                    try { retval = cursor.get(cont.key, data, Db.DB_NEXT); }
                    catch (DbException e) { BUG(e); }
                }
            }
            finish_get_by_time (cont, xact, cursor, data, retval,
                                req.comp_q, req.user_data);
        }
    }

    protected void finish_get_by_time (GBTCont cont, DbTxn xact, Dbc cursor,
                                       Dbt data, int retval,
                                       SinkIF comp_q, Object user_data) {

        LinkedList keys = new LinkedList (); 

        while (true) {
            if (retval != 0) {
                assert retval == Db.DB_NOTFOUND;
                GetByTimeResp resp = null;
                if (keys.isEmpty ())
                    resp = new GetByTimeResp(null, null, user_data);
                else
                    resp = new GetByTimeResp(keys, cont, user_data);
                application_enqueue (comp_q, resp);
                break;
            }

            Key k = new Key (cont.key);
            assert k.time_usec >= cont.low : k.time_usec + " " + cont.low;

            if (key_expired(k)) {
                // drop it, grab next one
                if (logger.isDebugEnabled())
                    logger.debug("dropping in do_get_by_time");
                // finish_get_by_guid does a partial get with data length
                // zero, since it's only going to return the key.  We need to
                // get all the data now so that we know how large it is.  This
                // will only happen once per tuple, though, so it's not as bad
                // as it might seem.
                data = new Dbt();
                try { retval = cursor.get(cont.key, data, Db.DB_CURRENT); }
                catch (DbException e) { BUG(e); }
                drop_expired_data (xact, cursor, k, data.getSize ());
                // the_real_handle_event (new DiscardReq(k, false));
                try { retval = cursor.get(cont.key, data, Db.DB_NEXT); }
                catch (DbException e) { BUG(e); }
                // return false;
                close_cursor (cursor);
                try { xact.commit(Db.DB_TXN_NOSYNC); }
                catch (DbException e) { BUG(e); }

                logger.info ("dropping key and reopenning xact");
                try { xact = env.txnBegin(null, Db.DB_TXN_NOSYNC); }
                catch (DbException e) { BUG(e); }
                cursor = open_cursor (by_time, xact, "finish_get_by_time");
                try { retval = cursor.get (cont.key, data, Db.DB_SET_RANGE); }
                catch (DbException e) { BUG(e); }
             }
            else {

                if (k.time_usec > cont.high) {
                    GetByTimeResp resp = null;
                    if (keys.isEmpty ())
                        resp = new GetByTimeResp(null, null, user_data);
                    else 
                        resp = new GetByTimeResp(keys, cont, user_data);
                    application_enqueue (comp_q, resp);
                    break;
                }

                keys.addLast (k);
                if (keys.size () >= 33) {
                    GetByTimeResp resp = 
                        new GetByTimeResp (keys, cont, user_data);
                    application_enqueue (comp_q, resp);
                    break;
                }
                else {
                    try { retval = cursor.get(cont.key, data, Db.DB_NEXT); }
                    catch (DbException e) { BUG(e); }
                }
            }
        }

        close_cursor (cursor);
        try { xact.commit(Db.DB_TXN_NOSYNC); }
        catch (DbException e) { BUG(e); }
    }

    protected void handle_iterate_by_guid_req (IterateByGuidReq req) {
        Key k = new Key (0L, 0, req.low, ZERO_HASH, ZERO_HASH, false, 
                         ZERO_CLIENT);
        byte [] kbuf = new byte [Key.SIZE];
        ByteBuffer bb = ByteBuffer.wrap (kbuf);
        k.to_byte_buffer (bb);
        primary_key_to_recycling_key (kbuf);
        Dbt key = new Dbt (kbuf);
        key.setSize (kbuf.length);
        Dbc cursor = open_cursor (by_guid, null,
                                              "handle_iterate_by_guid_req");
        IBGCont cont = new IBGCont (key, req.low, req.high);
        int retval = 0;
        Dbt pkey = new Dbt ();
        Dbt data = new Dbt ();
        try { retval = cursor.get (key, pkey, data, Db.DB_SET_RANGE);}
        catch (DbException e) { BUG(e); }

        while (! finish_iterate_by_guid (cont, cursor, pkey, data, retval, 
					 req.comp_q, req.user_data)) {
	    try { retval = cursor.get(key, pkey, data, Db.DB_NEXT); }
           catch (DbException e) { BUG(e); }
        }
    }

    protected void handle_iterate_by_guid_cont (IterateByGuidCont req) {

        if (req.comp_q != null) {
	    IBGCont cont = (IBGCont) req.continuation;
	    Dbc cursor = open_cursor (by_guid, null,
                                              "handle_iterate_by_guid_req");
	    int retval = 0;
	    Dbt pkey = new Dbt ();
	    Dbt data = new Dbt ();

            // Search to the last key returned.
            byte [] last_key = new byte [cont.key.getSize ()];
            System.arraycopy(cont.key.getData (), cont.key.getOffset (), 
                             last_key, 0, cont.key.getSize ());
            try { retval = cursor.get (cont.key, pkey, data, Db.DB_SET_RANGE);}
            catch (DbException e) { BUG(e); }

            // If we find it, get the next one.  If we find one that's already
            // greater than the last one (presumably because the last one was
            // removed from the DB), just go with that.
            if (retval == 0) {
                int cmp = arraycmp (cont.key.getData (), cont.key.getOffset (),
                                    last_key, 0, cont.key.getSize ());
		assert cmp >= 0;
                if (cmp == 0) {
                    try { retval = cursor.get (cont.key, pkey, data, Db.DB_NEXT); }
		    catch (DbException e) { BUG(e); }
                }
            }

	    while (! finish_iterate_by_guid (cont, cursor, pkey, data, retval, 
                                             req.comp_q, req.user_data)) {
		try { retval = cursor.get(cont.key, pkey, data, Db.DB_NEXT); }
		catch (DbException e) { BUG(e); }
	    }
        }
    }

    protected boolean finish_iterate_by_guid (IBGCont cont, Dbc cursor,
					      Dbt pkey, Dbt data, int retval,
                                              SinkIF comp_q, Object user_data) {

        if (retval == Db.DB_NOTFOUND) {
            if (logger.isDebugEnabled ()) logger.debug ("DB_NOTFOUND");
            IterateByGuidResp resp =
                new IterateByGuidResp (null, null, null, user_data);
            close_cursor (cursor);
            application_enqueue (comp_q, resp);
            return true;
        }

        Key k = new Key(pkey);
	assert k.guid.compareTo(cont.low) >= 0 : 
	  GuidTools.guid_to_string(k.guid) + " " + 
	  GuidTools.guid_to_string(cont.low);

        if (key_expired(k)) {
            // drop it, grab next
            if (logger.isDebugEnabled ())
                logger.debug("dropping in do_iterate_by_guid k=" +k);
            the_real_handle_event (new DiscardReq(k, false));
            return false;
        }

        if (k.guid.compareTo(cont.high) > 0) {
            if (logger.isDebugEnabled ())
                logger.debug ("iterate_by_guid is done. high guid=" +
                              GuidTools.guid_to_string (cont.high) +
                              ", which is less than last guid=" +
                              GuidTools.guid_to_string (k.guid));
            IterateByGuidResp resp =
                new IterateByGuidResp (null, null, null, user_data);
            close_cursor (cursor);
            application_enqueue (comp_q, resp);
        }
        else {
            ByteBuffer bb = ByteBuffer.wrap (data.getData (), data.getOffset (),
                                             data.getSize ());
            IterateByGuidResp resp =
                new IterateByGuidResp (k, bb, cont, user_data);
            close_cursor (cursor);
            application_enqueue (comp_q, resp);
        }

        return true;
    }

    protected void handle_discard_req (DiscardReq req) {

        // First, find the data.

        byte [] buf = new byte [Key.SIZE];
        req.key.to_byte_buffer (ByteBuffer.wrap (buf));
        Dbt key = new Dbt (buf);
        key.setSize (buf.length);
        Dbt data = new Dbt ();

        DbTxn xact = null;
        try { xact = env.txnBegin(null, Db.DB_TXN_NOSYNC); }
        catch (DbException e) { BUG(e); }

        int retval = 0;
        try { retval = by_time.get (xact, key, data, 0); }
        catch (DbException e) { BUG (e); }

        if (retval == 0) {

            try { retval = by_time.delete (xact, key, 0); }
            catch (DbException e) { BUG (e); }

            // Should be only one thread accessing the database.
            assert retval == 0 : retval;

            storage_changed(false /* removed */, req.key, data.getSize(), xact);

            if (logger.isInfoEnabled ()) {
                StringBuffer sbuf = new StringBuffer (105);
                if (req.recycle)
                    sbuf.append ("recycling key=0x");
                else
                    sbuf.append ("dropping expired key=0x");
                sbuf.append (GuidTools.guid_to_string (req.key.guid));
                sbuf.append (" time_usec=0x");
                sbuf.append (Long.toHexString (req.key.time_usec));
                sbuf.append (" ttl=");
                sbuf.append (req.key.ttl_sec);
                sbuf.append (" client_id=");
                sbuf.append (req.key.client_id.getHostAddress ());
                sbuf.append (" data_hash=0x");
                sbuf.append (bytes_to_str(req.key.data_hash, 0, 4));
                sbuf.append (" size=");
                sbuf.append (data.getSize ());
                logger.info (sbuf);
            }

            if (req.recycle) {

                // Swap the byte order of the key so that it's sorted by guid.

                byte[] sbuf = new byte[Key.SIZE];
                System.arraycopy(buf, 12, sbuf, 0, 20); // guid first
                System.arraycopy(buf, 0, sbuf, 20, 8); // then timestamp
                System.arraycopy(buf, 8, sbuf, 28, 4); // then ttl
                System.arraycopy(buf, 32, sbuf, 32, Key.SIZE - 32); // then rest
                Dbt skey = new Dbt(sbuf);
                skey.setSize(sbuf.length);

                // Add it to the recycling bin.

                try { retval = recycling.put(xact, skey, data, 0); }
                catch (DbException e) { BUG(e); }

                assert retval == 0 : retval;

            } // endif recycle

        } // endif get success
        else {
            assert retval == Db.DB_NOTFOUND : retval;
            if (logger.isDebugEnabled())
                logger.debug ("could not find key " + key);
        }

        try { xact.commit(Db.DB_TXN_NOSYNC); }
        catch (DbException e) { BUG(e); }
    }

    protected static void guid_to_bytes (
	    BigInteger guid, byte [] buf, int offset) {
        // Skip leading zero in guid bytes (due to twos-compliment).
        byte [] guid_bytes = guid.toByteArray ();
	if (guid_bytes.length < 20) {
	    System.arraycopy (guid_bytes, 0, buf,
		    offset + 20 - guid_bytes.length, guid_bytes.length);
	}
	else {
	    int guid_offset = (guid_bytes.length > 20) ? 1 : 0;
	    System.arraycopy (guid_bytes, guid_offset, buf, offset, 20);
	}
    }

    protected static class EnqueueEvent implements QueueElementIF {
        public SinkIF sink;
        public QueueElementIF item;
        public EnqueueEvent (SinkIF s, QueueElementIF i) { sink = s; item = i; }
    }

    protected void application_enqueue (SinkIF sink, QueueElementIF item) {
        if (logger.isDebugEnabled ())
            logger.debug ("enqueuing " + item + " to " + sink);
        classifier.dispatch_later (new EnqueueEvent (sink, item), 0);
    }

    protected byte [] data_hash (ByteBuffer data) {
        MessageDigest md = null;
	try { md = MessageDigest.getInstance ("SHA"); } catch (Exception e) {}
	byte [] bytes = data.array ();
	md.update (data.array (), data.arrayOffset (), data.limit ());
	return md.digest ();
    }
}

