/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.lss;
import java.io.FileOutputStream;
import java.security.SecureRandom;

/**
 * Creates a new file with a random key suitable for passing to
 * UdpCC.set_mac_key.
 */ 
public class CreateMacKeyFile {
    public static void main (String [] args) throws Exception {
        SecureRandom rand = SecureRandom.getInstance ("SHA1PRNG");
        byte [] keymat = new byte [20];
        rand.nextBytes (keymat);
        FileOutputStream os = new FileOutputStream (args [0]);
        os.write (keymat);
        os.close ();
    }
}

