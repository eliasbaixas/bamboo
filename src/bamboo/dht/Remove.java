/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import static org.acplt.oncrpc.OncRpcProtocols.*;

public class Remove {
    public static void main(String [] args) throws Exception {
        if (args.length != 6) {
            System.out.println("usage: java bamboo.dht.Remove <server_host> "
                               + "<server_port> <key> <value> <TTL> <secret>");
            System.exit(1);
        }
        String [] results = {"BAMBOO_OK", "BAMBOO_CAP", "BAMBOO_AGAIN"};
        InetAddress h = InetAddress.getByName(args[0]);
        int p = Integer.parseInt(args[1]);
        bamboo_rm_arguments removeArgs = new bamboo_rm_arguments();
        removeArgs.application = Put.class.getName();
        removeArgs.client_library = "Remote Tea ONC/RPC";
        MessageDigest md = MessageDigest.getInstance("SHA");
        removeArgs.key = new bamboo_key();
        // If key is of the form 0x12345678, parse it as a hexidecimal value
        // of the first 8 digits of the key, rather than hashing it.
        if (args[2].substring(0, 2).equals("0x")) {
            // Must use a long to handle keys whose first binary digit is 1.
            long keyPrefix = Long.parseLong(args[2].substring(2), 16);
            removeArgs.key.value = new byte[20];
            ByteBuffer.wrap(removeArgs.key.value).putInt((int) keyPrefix);
        }
        else {
            removeArgs.key.value = md.digest(args[2].getBytes());
        }
        removeArgs.value_hash = new bamboo_hash();
        removeArgs.value_hash.algorithm = "SHA";
        removeArgs.value_hash.hash = md.digest(args[3].getBytes());
        removeArgs.ttl_sec = Integer.parseInt(args[4]);
        removeArgs.secret_hash_alg = "SHA";
        removeArgs.secret = args[5].getBytes();
        gateway_protClient client = new gateway_protClient(h, p, ONCRPC_TCP);
        int result = client.BAMBOO_DHT_PROC_RM_3(removeArgs);
        System.out.println(results[result]);
    }
}

