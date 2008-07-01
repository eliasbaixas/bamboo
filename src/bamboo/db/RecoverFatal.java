/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.db;

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
import ostore.util.ByteUtils;
import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;

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
 * @version $Id: RecoverFatal.java,v 1.1 2004/09/01 18:26:30 srhea Exp $
 */
public class RecoverFatal {

    protected void primary_key_to_recycling_key (byte [] buf) {
        byte [] tmp = new byte [20];
        System.arraycopy (buf, 12, tmp, 0, 20); // back up guid
        System.arraycopy (buf, 0, buf, 20, 8); // move timestamp
        System.arraycopy (buf, 8, buf, 28, 4); // move ttl
        System.arraycopy (tmp, 0, buf, 0, 20); // restore guid
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
                result.setSize (40);
                return 0;
            }
        };

    public static void main (String [] args) throws Exception {
        new RecoverFatal (args [0]);
    }
     
    protected Db client_counts;
    protected Db by_time;
    protected Db by_guid;
    protected Db by_guid_and_data_hash;
    protected Db recycling;
    protected DbEnv env;

    public RecoverFatal (String homedir) throws Exception {

	// Initialize tables

        File directory = new File(homedir);

        // Create the directory if it doesn't exist.
        if (! directory.exists ()) {
            System.err.println ("no such directory: " + homedir);
            System.exit (1);
        }

        System.err.println ("openning env");
        // Open the environment.
        env = new DbEnv(0);
        env.setFlags(Db.DB_TXN_NOSYNC, true);
        env.open(homedir, Db.DB_INIT_MPOOL | Db.DB_INIT_TXN | Db.DB_INIT_LOCK | 
                Db.DB_RECOVER_FATAL | Db.DB_CREATE, 0);

        System.err.println ("openning dbs");
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

        System.err.println ("closing dbs");
        // Close it down.
        txn = env.txnBegin(null, Db.DB_TXN_SYNC);
        client_counts.close (0);
        by_time.close (0);
        by_guid.close (0);
        by_guid_and_data_hash.close (0);
        recycling.close (0);
        txn.commit(Db.DB_TXN_SYNC);

        System.err.println ("closing env");
        env.close (0);
    }
}

