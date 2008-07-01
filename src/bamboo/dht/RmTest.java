/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht;
import bamboo.util.StandardStage;
import java.security.MessageDigest;
import java.security.SecureRandom;
import seda.sandStorm.api.ConfigDataIF;
import static bamboo.util.Curry.*;
import static bamboo.util.StringUtil.*;

public class RmTest extends StandardStage {

    protected static final String APP = "bamboo.dht.RmTest $Revision: 1.2 $";
    protected GatewayClient client;
    protected bamboo_put_arguments putArgs;
    protected bamboo_get_args getArgs;
    protected byte[] secret;
    protected MessageDigest md;

    public RmTest() throws Exception {
        SecureRandom rand = new SecureRandom();

        putArgs = new bamboo_put_arguments();

        putArgs.application = APP;

        putArgs.key = new bamboo_key();
        putArgs.key.value = new byte[20];
        rand.nextBytes(putArgs.key.value);

        putArgs.value = new bamboo_value();
        putArgs.value.value = new byte[20];
        rand.nextBytes(putArgs.value.value);

        secret = new byte[20];
        rand.nextBytes(secret);
        md =  MessageDigest.getInstance("SHA");
        
        putArgs.secret_hash = new bamboo_hash();
        putArgs.secret_hash.algorithm = "SHA";
        putArgs.secret_hash.hash = md.digest(secret);

        putArgs.ttl_sec = 3600;
    }

    public void init(ConfigDataIF config) throws Exception {
        super.init(config);
        acore.registerTimer(0, ready);
        logger.info("key=0x" + bytes_to_str(putArgs.key.value));
        logger.info("secret=0x" + bytes_to_str(secret));
        logger.info("secret_hash=0x" + bytes_to_str(putArgs.secret_hash.hash));
        logger.info("value_hash=0x" + bytes_to_str(
                    md.digest(putArgs.value.value)));
    }

    public Runnable ready = new Runnable() {
        public void run() {
            client = GatewayClient.instance(my_node_id);
            client.putSecret(putArgs, putDone);
        }
    };

    public Thunk1<Integer> putDone = new Thunk1<Integer>() {
        public void run(Integer result) {
            if (result.intValue() != 0) {
                logger.warn("trying put again");
                acore.registerTimer(1000, ready);
            }
            else {
                logger.info("put successful");
                getArgs = new bamboo_get_args();
                getArgs.application = APP;
                getArgs.key = putArgs.key;
                getArgs.maxvals = 1;
                getArgs.placemark = new bamboo_placemark();
                getArgs.placemark.value = new byte[0];
                client.getDetails(getArgs, checkThere);
            }
        }
    };

    public Thunk1<bamboo_get_result> checkThere = 
    new Thunk1<bamboo_get_result>() {
        public void run(bamboo_get_result getResults) {
            if (getResults.values.length <= 0) {
                logger.fatal("not there");
                System.exit(1);
            }
            logger.info("get successful");
            bamboo_rm_arguments rmArgs = new bamboo_rm_arguments();
            rmArgs.application = APP;
            rmArgs.key = putArgs.key;
            rmArgs.value_hash = new bamboo_hash();
            rmArgs.value_hash.algorithm = "SHA";
            rmArgs.value_hash.hash = md.digest(putArgs.value.value);
            rmArgs.secret_hash_alg = "SHA";
            rmArgs.secret = secret;
            rmArgs.ttl_sec = 3600;
            client.remove(rmArgs, removeDone);
        }
    };

    public Thunk1<Integer> removeDone = new Thunk1<Integer>() {
        public void run(Integer result) {
            if (result.intValue() != 0) {
                logger.fatal("remove failed");
                System.exit(1);
            }
            logger.info("remove successful");
            client.getDetails(getArgs, checkGone);
        }
    };

    public Thunk1<bamboo_get_result> checkGone = 
    new Thunk1<bamboo_get_result>() {
        public void run(bamboo_get_result getResults) {
            if (getResults.values.length > 0) {
                logger.fatal("still there");
                System.exit(1);
            }
            logger.info("get successful");
            System.exit(0);
        }
    };
}

