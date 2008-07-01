 /*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.openhash.multicast;
import bamboo.util.StandardStage;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import ostore.util.NodeId;
import ostore.network.NetworkMessageResult;

public class CheckRunning extends StandardStage
implements SingleThreadedEventHandlerIF {

    public CheckRunning () {
        event_types = new Class [] { NetworkMessageResult.class };
    }

    public void init (ConfigDataIF config) throws Exception {
	super.init (config);
        NodeId node = new NodeId (config_get_string (config, "node"));
        MulticastPingMessage outb = new MulticastPingMessage (node);
        outb.comp_q = my_sink;
        outb.timeout_sec = 60;
        dispatch (outb);
    }

    public void handleEvent (QueueElementIF item) {
        if (item instanceof NetworkMessageResult) {
            NetworkMessageResult result = (NetworkMessageResult) item;
            if (result.success) {
                logger.info ("got NetworkMessageResult success");
                System.exit (0);
            }
            else {
                logger.info ("got NetworkMessageResult failure");
                System.exit (1);
            }
        }
        else {
            assert false : item.getClass ().getName ();
        }
    }
}
