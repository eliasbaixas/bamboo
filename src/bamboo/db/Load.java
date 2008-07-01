/*
 * Copyright (c) 2001-2005 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.db;
import bamboo.lss.ASyncCore;
import bamboo.util.StandardStage;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import seda.sandStorm.api.SinkException;
import seda.sandStorm.api.SinkIF;
import static bamboo.db.StorageManager.PutReq;
import static bamboo.db.StorageManager.PutResp;
import static bamboo.db.StorageManager.ZERO_HASH;
import static bamboo.util.Curry.*;

public class Load extends StandardStage {

    protected int putsOutstanding, loaded;
    protected FileInputStream is;
    protected boolean readDone;

    public Load() {
        event_types = new Class [] { PutResp.class };
    }

    public void init(ConfigDataIF config) throws Exception {
        super.init(config);
        is = new FileInputStream(config_get_string(config, "input_file"));
        acore.registerTimer(0, ready);
    }

    public Runnable ready = new Runnable() {
        public void run() {
            logger.info("starting load");
            readNext();
            if (readDone && putsOutstanding == 0) {
                logger.info("databases loaded successfully");
                System.exit(0);
            }
        }
    };

    public void readNext() {
        assert !readDone;
        while (putsOutstanding < 100) {
            try {
                byte[] kbytes = new byte[OldStorageManager.Key.SIZE];
                int i = is.read(kbytes);
                if (i == -1) {
                    readDone = true;
                    break;
                }
                OldStorageManager.Key oldKey = 
                    new OldStorageManager.Key(ByteBuffer.wrap(kbytes));
                byte[] lbytes = new byte[4];
                is.read(lbytes);
                int len = ByteBuffer.wrap(lbytes).getInt();
                byte[] data = new byte[len];
                is.read(data);
                StorageManager.Key newKey = new StorageManager.Key(
                        oldKey.time_usec, oldKey.ttl_sec, oldKey.guid, 
                        ZERO_HASH, oldKey.data_hash, oldKey.put, 
                        oldKey.client_id);
                dispatch(new PutReq(newKey, ByteBuffer.wrap(data), 
                                    my_sink, null));
                ++putsOutstanding;
            }
            catch (IOException e) {
                logger.fatal("caught " + e);
                System.exit(1);
            }
        }
    }

    public void handleEvent(QueueElementIF item) {
        PutResp resp = (PutResp) item;
        if (resp.inval_put_key != null)
            logger.warn("inval_put_key=" + resp.inval_put_key);
        if (resp.inval_rm_key != null)
            logger.warn("inval_rm_key=" + resp.inval_rm_key);
        --putsOutstanding;
        ++loaded;
        if (loaded % 1000 == 0) {
            logger.info(loaded + " records loaded");
        }

        if (! readDone)
            readNext();
        if (readDone && putsOutstanding == 0) {
            logger.info("databases loaded successfully");
            System.exit(0);
        }
    }
}

