/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dmgr;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Random;
import java.util.LinkedList;
import ostore.util.ByteUtils;
import ostore.util.ByteArrayOutputBuffer;
import ostore.util.Pair;
import bamboo.util.StandardStage;
import ostore.util.CountBuffer;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import seda.sandStorm.api.StagesInitializedSignal;
import bamboo.db.StorageManager;
import bamboo.util.GuidTools;

/**
 * A simple regression test for the DataManager.
 *
 * @author Sean C. Rhea
 * @version $Id: DataManagerTest.java,v 1.25 2005/05/12 00:08:19 srhea Exp $
 */
public class DataManagerTest extends StandardStage
implements SingleThreadedEventHandlerIF {

    protected Random rand;
    protected int to_put, put_size;
    public DataManagerTest () throws Exception {
        DEBUG = false;
        event_types = new Class [] {
            StagesInitializedSignal.class,
            PrintDataAlarm.class
        };
    }

    public void init (ConfigDataIF config) throws Exception {
        super.init (config);
        int debug_level = config.getInt ("debug_level");
        if (debug_level > 0)
            DEBUG = true;
        rand = new Random (my_node_id.hashCode ());
        to_put = config.getInt ("to_put");
        if (to_put == -1)
            to_put = 1;
        put_size = config.getInt ("put_size");
        if (put_size == -1)
            put_size = 100;
    }

    public BigInteger random_guid () {
        return GuidTools.random_guid (rand);
    }

    protected ByteBuffer random_data (int size) {
	byte [] data_bytes = new byte [size];
	rand.nextBytes (data_bytes);
	return ByteBuffer.wrap (data_bytes);
    }

    /*
    protected void do_put (int ttl_sec) {
        long now_usec = now_ms () * 1000;

	CountBuffer cb = new CountBuffer ();
	cb.add (my_node_id);
	byte [] data_bytes = new byte [cb.size ()];
	ByteArrayOutputBuffer ob = new ByteArrayOutputBuffer (data_bytes);
	ob.add (my_node_id);

        BigInteger guid = random_guid ();
        ByteBuffer data_buf = ByteBuffer.wrap (data_bytes);

	PutOrRemoveReq outb = new PutOrRemoveReq(now_usec, ttl_sec, 
                guid, data_buf, null, true, StorageManager.ZERO_CLIENT, 
                my_sink, null);

        dispatch (outb);
    }
    */

    public void handleEvent (QueueElementIF item) {
	if (logger.isDebugEnabled ()) logger.debug ("got " + item);

        if (item instanceof StagesInitializedSignal) {
            /*
	    if (to_put > 0)
		do_put (30);   // 30-second ttl for one of them
            */
            dispatch (new PrintDataAlarm ());
        }
        else if (item instanceof PutOrRemoveResp) {
            /*
            --to_put;
            if (to_put > 0)
                do_put (3600);
            */
        }
        else if (item instanceof PrintDataAlarm) {
            print_data ();
        }
        else if (item instanceof StorageManager.GetByTimeResp) {
            handle_get_by_time_resp ((StorageManager.GetByTimeResp) item);
        }
        else if (item instanceof StorageManager.GetByGuidResp) {
            handle_get_by_guid_resp ((StorageManager.GetByGuidResp) item);
        }
        else {
            throw new IllegalArgumentException (item.getClass ().getName ());
        }
    }

    protected static class PrintDataAlarm implements QueueElementIF {}

    protected void print_data () {
        // Start with the primary DB.

        dispatch (new StorageManager.GetByTimeReq (
                    0, Long.MAX_VALUE, my_sink, new LinkedList ()));
    }

    protected void handle_get_by_time_resp (StorageManager.GetByTimeResp resp) {

        if (resp.continuation == null) {

            // Also scan the recycling DB, since we want to make sure that no
            // node is storing tuples that it shouldn't be.

            StorageManager.GetByGuidReq req = new StorageManager.GetByGuidReq (
                    BigInteger.valueOf (0), false,null,my_sink, resp.user_data);
            dispatch (req);
        }
        else {
            for (StorageManager.Key k : resp.keys) {
                if (k.put) {
                    LinkedList data = (LinkedList) resp.user_data;
                    data.addLast (new Pair (k, null /*resp.data*/));
                }
            }

            dispatch (new StorageManager.GetByTimeCont (
                        resp.continuation, my_sink, resp.user_data));
        }
    }

    protected void handle_get_by_guid_resp (StorageManager.GetByGuidResp resp) {
        if (resp.continuation == null) {
            // All done.
            System.err.println ("DataManagerTest-" + my_node_id
                    + " stored data:");
            LinkedList data = (LinkedList) resp.user_data;
            for (Iterator i = data.iterator (); i.hasNext (); ) {
                Pair p = (Pair) i.next ();
                StorageManager.Key k = (StorageManager.Key) p.first;
                /*
                ByteBuffer d = (ByteBuffer) p.second;
                ByteArrayInputBuffer ib = new ByteArrayInputBuffer (
                        d.array (), d.arrayOffset (), d.limit ());
                NodeId src = null;
                try {
                    src = (NodeId) ib.nextObject ();
                }
                catch (Exception e) {
                    BUG ("caught " + e);
                }
                */
                System.err.println ("  0x" +
                        GuidTools.guid_to_string (k.guid) + " 0x" +
                        ByteUtils.print_bytes (k.data_hash, 0, 4));
            }
            classifier.dispatch_later (new PrintDataAlarm (), 10*1000);
        }
        else {
            if (resp.key.put) {
                LinkedList data = (LinkedList) resp.user_data;
                data.addLast (new Pair (resp.key, resp.data));
            }
            dispatch (new StorageManager.GetByGuidCont (
                        resp.continuation, false, my_sink, resp.user_data));
        }
    }
}

