/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.router;
import bamboo.util.StandardStage;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.StagesInitializedSignal;
import java.util.Random;
import java.math.BigInteger;
import ostore.util.NodeId;
import bamboo.util.GuidTools;
import bamboo.lss.ASyncCore;

public class LookupTest extends bamboo.util.StandardStage
implements SingleThreadedEventHandlerIF {

    protected Router router;
    protected Random rand;
    protected double mean_period_ms;

    protected static double random_exponential(double mean, Random rand) {
        double u = rand.nextDouble();
        return (0 - (mean * Math.log(1.0 - u)));
    }

    public void init(ConfigDataIF config) throws Exception {
        super.init(config);
        int seed = config_get_int (config, "seed");
        if (seed != -1)
             rand = new Random (seed);
        mean_period_ms = config_get_int(config, "mean_period_ms");
        if (mean_period_ms == -1.0)
            mean_period_ms = 60*1000;
        long start_delay_ms = config_get_int (config, "start_delay_ms");
        if (start_delay_ms == -1)
            start_delay_ms = 0;
        acore.register_timer (start_delay_ms, ready_cb, null);
    }

    public ASyncCore.TimerCB ready_cb = new ASyncCore.TimerCB () {
        public void timer_cb (Object user_data) {
            router = Router.instance(my_node_id);
            try {
                router.registerApplication(
                        Router.applicationID(this.getClass()),
                        null, null, null, null, null);
            }
            catch (Router.DuplicateApplicationException e) {
                BUG(e);
            }
            if (rand == null)
                rand = new Random (router.id().hashCode () ^ (int) now_ms ());
            next_op ();
         }
    };

    public void next_op () {
        long wait = Math.round (random_exponential (mean_period_ms, rand));
        acore.register_timer(wait, next_op_cb, null);
    }

    public ASyncCore.TimerCB next_op_cb = new ASyncCore.TimerCB () {
        public void timer_cb(Object user_data) {
            byte[] guid_bytes = new byte[21];
            rand.nextBytes(guid_bytes);
            guid_bytes[0] = 0;
            BigInteger guid = new BigInteger(guid_bytes);
            logger.info("looking up 0x" + GuidTools.guid_to_string(guid) + ".");
            Long start_time = new Long(timer_ms());
            router.lookup(guid, lookup_cb, start_time);
            next_op ();
        }
    };

    public Router.LookupCb lookup_cb = new Router.LookupCb () {
        public void lookup_cb (BigInteger lookup_id, BigInteger closest_id,
                               NodeId closest_addr, Object user_data) {
            long finish_time = timer_ms ();
            long start_time = ((Long) user_data).longValue();
            long latency_ms = finish_time - start_time;
            logger.info ("found 0x" + GuidTools.guid_to_string (lookup_id) +
                         " on 0x" + GuidTools.guid_to_string (closest_id) +
                         ", " + closest_addr + " in " + latency_ms + " ms.");
        }
    };
}
