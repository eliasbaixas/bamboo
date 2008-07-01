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

public class Put {
    public static void main(String [] args) throws Exception {
        if (args.length < 5) {
            System.out.println("usage: java bamboo.dht.Put <server_host> "
                               + "<server_port> <key> <value> <TTL> [secret]");
            System.exit(1);
        }
        String [] results = {"BAMBOO_OK", "BAMBOO_CAP", "BAMBOO_AGAIN"};
        InetAddress h = InetAddress.getByName(args[0]);
        int p = Integer.parseInt(args[1]);
        bamboo_put_arguments putArgs = new bamboo_put_arguments();
        putArgs.application = Put.class.getName();
        putArgs.client_library = "Remote Tea ONC/RPC";
        MessageDigest md = MessageDigest.getInstance("SHA");
        putArgs.key = new bamboo_key();
        // If key is of the form 0x12345678, parse it as a hexidecimal value
        // of the first 8 digits of the key, rather than hashing it.
        if (args[2].substring(0, 2).equals("0x")) {
            // Must use a long to handle keys whose first binary digit is 1.
            long keyPrefix = Long.parseLong(args[2].substring(2), 16);
            putArgs.key.value = new byte[20];
            ByteBuffer.wrap(putArgs.key.value).putInt((int) keyPrefix);
        }
        else {
            putArgs.key.value = md.digest(args[2].getBytes());
        }
        putArgs.value = new bamboo_value();
        putArgs.value.value = args[3].getBytes();
        putArgs.ttl_sec = Integer.parseInt(args[4]);
        putArgs.secret_hash = new bamboo_hash();
        if (args.length > 5) {
            putArgs.secret_hash.algorithm = "SHA";
            putArgs.secret_hash.hash = md.digest(args[5].getBytes());
        }
        else {
            putArgs.secret_hash.algorithm = "";
            putArgs.secret_hash.hash = new byte[0];
        }
        gateway_protClient client = new gateway_protClient(h, p, ONCRPC_TCP);
        int result = client.BAMBOO_DHT_PROC_PUT_3(putArgs);
        System.out.println(results[result]);
    }
}

