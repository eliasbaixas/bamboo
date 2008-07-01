/*
 * Copyright (c) 2001-2005 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.db;
import bamboo.lss.ASyncCore;
import bamboo.util.StandardStage;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import seda.sandStorm.api.SinkException;
import seda.sandStorm.api.SinkIF;
import static bamboo.db.OldStorageManager.GetByTimeCont;
import static bamboo.db.OldStorageManager.GetByTimeReq;
import static bamboo.db.OldStorageManager.GetByTimeResp;
import static bamboo.db.OldStorageManager.GetByKeyReq;
import static bamboo.db.OldStorageManager.GetByKeyResp;
import static bamboo.db.StorageManager.PutReq;
import static bamboo.db.StorageManager.PutResp;
import static bamboo.db.StorageManager.ZERO_HASH;
import static bamboo.util.Curry.*;

public class StorageConverter extends StandardStage {

    protected int getsOutstanding, putsOutstanding, converted;
    protected Object getContinuation;

    public StorageConverter() {
        event_types = new Class [] { 
            PutResp.class, 
            GetByTimeResp.class,
            GetByKeyResp.class
        };
    }

    public void init(ConfigDataIF config) throws Exception {
        super.init(config);
        acore.registerTimer(0, ready);
    }

    public Runnable ready = new Runnable() {
        public void run() {
            logger.info("starting conversion");
            dispatch(new GetByTimeReq(0, Long.MAX_VALUE, my_sink, null));
        }
    };

    public void handleEvent(QueueElementIF item) {
        if (item instanceof GetByTimeResp) {
            GetByTimeResp resp = (GetByTimeResp) item;
            if (resp.keys != null) {
                for (OldStorageManager.Key k : resp.keys) {
                    ++getsOutstanding;
                    dispatch(new GetByKeyReq(k, my_sink, null));
                }
            }
            getContinuation = resp.continuation;
        }
        else if (item instanceof GetByKeyResp) {
            GetByKeyResp resp = (GetByKeyResp) item;
            --getsOutstanding;
            ++putsOutstanding;
            StorageManager.Key newKey = new StorageManager.Key(
                    resp.key.time_usec, resp.key.ttl_sec, resp.key.guid, 
                    ZERO_HASH, resp.key.data_hash, resp.key.put, 
                    resp.key.client_id);
            dispatch(new PutReq(newKey, resp.data, my_sink, null));
        }
        else {
            PutResp resp = (PutResp) item;
            if (resp.inval_put_key != null)
                logger.warn("inval_put_key=" + resp.inval_put_key);
            if (resp.inval_rm_key != null)
                logger.warn("inval_rm_key=" + resp.inval_rm_key);
            --putsOutstanding;
            ++converted;
            if (converted % 1000 == 0) {
                logger.info(converted + " records converted");
            }
        }

        if (getContinuation != null && getsOutstanding < 1000 
            && putsOutstanding < 1000) {
            dispatch(new GetByTimeCont(getContinuation, my_sink, null));
        }
        if (getsOutstanding == 0 && putsOutstanding == 0 
            && getContinuation == null) {
            logger.info("databases transferred successfully");
            System.exit(0);
        }
    }
}

