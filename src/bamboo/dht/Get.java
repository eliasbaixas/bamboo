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

public class Get {
    public static void main(String [] args) throws Exception {
        if (args.length < 3) {
            System.out.println("usage: java bamboo.dht.Get <server_host> "
                               + "<server_port> <key> [max_vals]");
            System.exit(1);
        }
        InetAddress h = InetAddress.getByName(args[0]);
        int p = Integer.parseInt(args[1]);
        bamboo_get_args getArgs = new bamboo_get_args();
        getArgs.application = Get.class.getName();
        getArgs.client_library = "Remote Tea ONC/RPC";
        MessageDigest md = MessageDigest.getInstance("SHA");
        getArgs.key = new bamboo_key();
        // If key is of the form 0x12345678, parse it as a hexidecimal value
        // of the first 8 digits of the key, rather than hashing it.
        if (args[2].substring(0, 2).equals("0x")) {
            // Must use a long to handle keys whose first binary digit is 1.
            long keyPrefix = Long.parseLong(args[2].substring(2), 16);
            getArgs.key.value = new byte[20];
            ByteBuffer.wrap(getArgs.key.value).putInt((int) keyPrefix);
        }
        else {
            getArgs.key.value = md.digest(args[2].getBytes());
        }
        if (args.length > 3) 
            getArgs.maxvals = Integer.parseInt(args[3]);
        else
            getArgs.maxvals = Integer.MAX_VALUE;
        getArgs.placemark = new bamboo_placemark();
        getArgs.placemark.value = new byte [0];
        gateway_protClient client = new gateway_protClient(h, p, ONCRPC_TCP);
        while (true) {
            bamboo_get_res res = client.BAMBOO_DHT_PROC_GET_2(getArgs);
            for (int i = 0; i < res.values.length; ++i)
                System.out.println(new String (res.values[i].value));
            if (res.placemark.value.length == 0)
                break;
            getArgs.placemark = res.placemark;
        }
    }
}

