/*
 * Copyright (c) 2005 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.fst;
import java.util.Arrays;

/**
 * As far as the FST algorithm is concerned, clients are identified by
 * arbitrary byte arrays.
 */ 
public class Client {
    protected byte [] bytes;
    public Client(byte [] bytes) { this.bytes = bytes; }
    public byte [] getBytes() { return bytes; }
    public boolean equals(Object rhs) {
        Client other = (Client) rhs;
        return Arrays.equals(bytes, other.bytes);
    }
    public int hashCode() {
        int result = 0;
        for (int i = 0; i < bytes.length; ++i)
            result = (result << 4) | (0xff & (int) bytes[i]);
        return result;
        // This function isn't present pre-Java 1.5
        // return Arrays.hashCode(bytes);
    }
    public String toString() {
        return "0x" + bamboo.util.StringUtil.bytes_to_str(bytes);
    }
}

