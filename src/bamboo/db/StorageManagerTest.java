/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.db;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Random;
import ostore.util.Clock;
import ostore.util.Debug;
import bamboo.util.StandardStage;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.EventHandlerIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import seda.sandStorm.api.SinkException;
import seda.sandStorm.api.StagesInitializedSignal;
import bamboo.util.GuidTools;
import static bamboo.db.StorageManager.*;

/**
 * A simple regression test for the StorageManager.
 *
 * @author Sean C. Rhea
 * @version $Id: StorageManagerTest.java,v 1.20 2005/05/20 22:38:10 srhea Exp $
 */
public class StorageManagerTest extends StandardStage
implements SingleThreadedEventHandlerIF {

    protected Random rand;
    protected MessageDigest md;
    protected int count;

    public StorageManagerTest () {
        DEBUG = false;
        event_types = new Class [] {
            StagesInitializedSignal.class,
            StorageManager.PutResp.class,
            StorageManager.GetByGuidResp.class,
            StorageManager.GetByTimeResp.class
        };
    }

    public void init (ConfigDataIF config) throws Exception {
        super.init (config);
        int debug_level = config.getInt ("debug_level");
        if (debug_level > 0)
            DEBUG = true;
        rand = new Random (1);
        md = MessageDigest.getInstance ("SHA");
    }

    public BigInteger random_guid () {
        return GuidTools.random_guid (rand);
    }

    protected ByteBuffer random_data (int size) {
	byte [] data_bytes = new byte [size];
	rand.nextBytes (data_bytes);
	return ByteBuffer.wrap (data_bytes);
    }

    protected byte [] data_hash (ByteBuffer data) {
	byte [] bytes = data.array ();
	md.update (data.array (), data.arrayOffset (), data.limit ());
	return md.digest ();
    }

    protected long now;
    protected BigInteger small_guid;

    public void handleEvent (QueueElementIF item) {
	if (logger.isDebugEnabled ()) logger.debug ("got " + item);

        if (item instanceof StagesInitializedSignal) {
	    now = now_ms () * 1000;

	    small_guid = random_guid ();
	    BigInteger large_guid = random_guid ();
	    if (small_guid.compareTo (large_guid) > 0) {
		BigInteger tmp = small_guid;
		small_guid = large_guid;
		large_guid = tmp;
	    }

            ByteBuffer data = random_data (4096);
            StorageManager.Key key = new StorageManager.Key (
                    now, 3660, small_guid, ZERO_HASH, data_hash (data), true,
                    StorageManager.ZERO_CLIENT);

            dispatch (new StorageManager.PutReq (key, data, my_sink, null));

            data = random_data (4096);
            key = new StorageManager.Key (
                    now, 3660, large_guid, ZERO_HASH, data_hash (data), true,
                    StorageManager.ZERO_CLIENT);

            dispatch (new StorageManager.PutReq (key, data, my_sink, null));

            data = random_data (4096);
            key = new StorageManager.Key (
                    now + 1, 3660, small_guid, ZERO_HASH, data_hash (data), 
                    true, StorageManager.ZERO_CLIENT);

            dispatch (new StorageManager.PutReq (key, data, my_sink, null));

        }
        else if (item instanceof StorageManager.PutResp) {
            ++count;
            if (count == 3) {
		StorageManager.PutResp resp = (StorageManager.PutResp) item;
                StorageManager.GetByGuidReq req =
                    new StorageManager.GetByGuidReq (
                            small_guid, true, null, my_sink, null);
                dispatch (req);
            }
        }
        else if (item instanceof StorageManager.GetByGuidResp) {
            StorageManager.GetByGuidResp resp =
                (StorageManager.GetByGuidResp) item;
            if (resp.continuation == null) {
		StorageManager.GetByTimeReq req =
		    new StorageManager.GetByTimeReq (
                            now, now + 1, my_sink, null);
                dispatch (req);
	    }
	    else {
                dispatch (new StorageManager.GetByGuidCont (
                            resp.continuation, my_sink, null));
	    }
        }
        else if (item instanceof StorageManager.GetByTimeResp) {
            StorageManager.GetByTimeResp resp =
                (StorageManager.GetByTimeResp) item;
            if (resp.continuation != null) {
                dispatch (new StorageManager.GetByTimeCont (
                            resp.continuation, my_sink, null));
	    }
        }
        else {
            throw new IllegalArgumentException (item.getClass ().getName ());
        }
    }
}

