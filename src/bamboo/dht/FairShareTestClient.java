/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht;
import bamboo.lss.ASyncCore;
import bamboo.lss.DustDevil;
import bamboo.util.StandardStage;
import java.util.Random;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import ostore.util.ByteUtils;

/**
 * Puts at a given rate, for use in testing fair sharing.  Does not keep track
 * of what or how much it has put.
 *
 * @author Sean C. Rhea
 * @version $Id: FairShareTestClient.java,v 1.2 2004/11/13 18:35:16 srhea Exp $
 */
public class FairShareTestClient extends StandardStage
implements SingleThreadedEventHandlerIF {

    protected Random rand;
    protected GatewayClient client;
    protected bamboo.lss.ASyncCore acore;
    protected double mean_wait_ms;
    protected int put_size;
    protected int put_ttl_sec;

    protected static double random_exponential (double mean, Random rand) {
        double u = rand.nextDouble ();
        return (0 - (mean * Math.log (1.0 - u)));
    }

    protected void next_op () {
        long wait_ms = Math.round (random_exponential (mean_wait_ms, rand));
        acore.register_timer (wait_ms, do_put_cb, null);
    }

    public ASyncCore.TimerCB do_put_cb = new ASyncCore.TimerCB () {
	public void timer_cb (Object user_data) {
            bamboo_put_args put = new bamboo_put_args();
            put.application = "bamboo.dht.FairShareTestClient $Revision: 1.2 $";
            // GatewayClient will fill in put.client_library
            put.value = new bamboo_value();
            put.value.value = new byte[put_size];
            put.key = new bamboo_key();
            put.key.value = new byte[20];
            rand.nextBytes(put.key.value);
            put.ttl_sec = put_ttl_sec;
            logger.info ("sending put with key 0x" +
                         ByteUtils.print_bytes(put.key.value, 0, 4));
            client.put(put, put_done_cb, put.key.value);
            next_op ();
        }
    };

    public GatewayClient.PutDoneCb put_done_cb =new GatewayClient.PutDoneCb () {
        public void put_done_cb (int put_res, Object user_data) {
            logger.info ("put with key 0x" +
                         ByteUtils.print_bytes((byte []) user_data, 0, 4) +
                         (put_res == bamboo_stat.BAMBOO_OK ?
                          "succeeded" : "failed"));
        }
    };

    public void init (ConfigDataIF config) throws Exception {
	super.init (config);
        mean_wait_ms = config_get_int (config, "mean_wait_ms");
        put_size = config_get_int (config, "put_size");
        put_ttl_sec = config_get_int (config, "put_ttl_sec");
        int seed = config_get_int (config, "seed");
        rand = new Random (seed);
        acore = DustDevil.acore_instance ();
        String client_stg_name =
            config_get_string (config, "client_stage_name");
        client = (GatewayClient) lookup_stage (config, client_stg_name);
        next_op ();
    }

    public void handleEvent (QueueElementIF item) { BUG ("got: " + item); }
}

