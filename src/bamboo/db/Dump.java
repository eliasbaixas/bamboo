/*
 * Copyright (c) 2001-2005 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.db;
import bamboo.lss.ASyncCore;
import bamboo.util.StandardStage;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
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
import static bamboo.db.StorageManager.ZERO_HASH;
import static bamboo.util.Curry.*;

public class Dump extends StandardStage {

    protected int getsOutstanding, dumped;
    protected Object getContinuation;
    protected FileOutputStream os;

    public Dump() {
        event_types = new Class [] { 
            GetByTimeResp.class,
            GetByKeyResp.class
        };
    }

    public void init(ConfigDataIF config) throws Exception {
        super.init(config);
        os = new FileOutputStream(config_get_string(config, "output_file"));
        acore.registerTimer(0, ready);
    }

    public Runnable ready = new Runnable() {
        public void run() {
            logger.info("starting dump");
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
            if (resp.data != null) {
                byte[] kbytes = new byte[OldStorageManager.Key.SIZE];
                resp.key.to_byte_buffer(ByteBuffer.wrap(kbytes));
                try {
                    os.write(kbytes, 0, kbytes.length);
                    byte[] len = new byte[4];
                    ByteBuffer.wrap(len).putInt(
                            resp.data.limit() - resp.data.position());
                    os.write(len, 0, len.length);
                    os.write(resp.data.array(), 
                            resp.data.arrayOffset() + resp.data.position(), 
                            resp.data.limit() - resp.data.position());
                }
                catch (IOException e) {
                    logger.fatal("caught " + e);
                    System.exit(1);
                }
                ++dumped;
                if (dumped % 1000 == 0) {
                    logger.info(dumped + " records dumped");
                }
            }
        }

        if (getContinuation != null && getsOutstanding < 100) {
            dispatch(new GetByTimeCont(getContinuation, my_sink, null));
        }
        if (getsOutstanding == 0 && getContinuation == null) {
            try {
                os.close();
            }
            catch (IOException e) {
                logger.fatal("caught " + e);
                System.exit(1);
            }
            logger.info("databases dumped successfully");
            System.exit(0);
        }
    }
}

