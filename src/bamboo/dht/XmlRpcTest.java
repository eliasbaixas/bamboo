/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht;
import java.util.Arrays;
import java.util.Random;
import java.util.Vector;
import org.apache.xmlrpc.XmlRpcClientLite;

public class XmlRpcTest {
    public static void main (String [] args) throws Exception {
        XmlRpcClientLite client = new XmlRpcClientLite (args [0]);
        Vector params = new Vector ();

        Random rand = new Random (1);
        byte [] key = new byte [20];
        rand.nextBytes (key);
        params.add (key);

        byte [] value = new byte [64];
        rand.nextBytes (value);
        params.add (value);

        Integer ttl_sec = new Integer (120);
        params.add (ttl_sec);

        String application = "XmlRpcTest";
        params.add (application);

        System.out.println ("Doing a put.");
        Integer put_result = (Integer) client.execute ("put", params);
        if (put_result == bamboo.dht.bamboo_stat.BAMBOO_OK)
            System.out.println ("Put succeeded.");
        else 
            System.out.println ("Put failed.");

        params = new Vector ();
        params.add (key);

        Integer maxvals = new Integer (1);
        params.add (maxvals);

        byte [] placemark_bytes = new byte [0];
        params.add (placemark_bytes);

        params.add (application);

        System.out.println ("Doing a get.");
        Vector get_result = (Vector) client.execute ("get", params);

        Vector values = (Vector) get_result.elementAt (0);
        for (Object obj : values) {
            byte [] returned_bytes = (byte []) obj;
            if (Arrays.equals (returned_bytes, value)) {
                System.out.println ("Get succeeded.");
                System.exit (0);
            }
        }

        System.out.println ("Get failed.");
    }
}

