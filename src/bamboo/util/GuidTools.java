/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import ostore.util.ByteArrayInputBuffer;
import ostore.util.ByteArrayOutputBuffer;
import ostore.util.Carp;
import ostore.util.CountBuffer;
import ostore.util.Debug;
import ostore.util.DebugFlags;
import ostore.util.NodeId;
import ostore.util.Pair;
import ostore.util.QuickSerializable;
import ostore.util.SHA1Hash;
import ostore.util.SecureHash;
import bamboo.util.StandardStage;

/**
 * GuidTools.
 *
 * @author  Sean C. Rhea
 * @version $Id: GuidTools.java,v 1.16 2003/12/20 02:44:19 srhea Exp $
 */
public class GuidTools {

    public static BigInteger random_guid (Random rand) {
	byte [] guid_bytes = new byte [21];
	rand.nextBytes (guid_bytes);
	guid_bytes [0] = 0; // account for twos compliment
	return new BigInteger (guid_bytes);
    }

    /**
     * Print only the high-order 8 hexedecimal digits.
     */
    public static String guid_to_string (BigInteger i) {
	// Print only the high-order 8 hexedecimal digits.
	String result = i.toString (16);
	while (result.length () < 40) 
	    result = "0" + result;
	result = result.substring (0, 8);
	return result;
    }

    public static BigInteger secure_hash_to_big_integer (SecureHash x) {
	CountBuffer cb = new CountBuffer ();
	x.serialize (cb);
	byte [] data = new byte [cb.size () + 1];
	ByteArrayOutputBuffer ob = new ByteArrayOutputBuffer (data);
	// Prepend a zero to force this to be a positive number in twos
	// compliment form.
	ob.add ((byte) 0);
	x.serialize (ob);
	return new BigInteger (data);
    }

    public static SecureHash big_integer_to_secure_hash (BigInteger i) {
	byte [] bytes = i.toByteArray ();
	ByteArrayInputBuffer ib = null;
	if (bytes.length > 20) {
	    if ((bytes.length != 21) || (bytes [0] != 0))
		Carp.die ("Bad format");
	    ib = new ByteArrayInputBuffer (bytes, bytes.length - 20, 20);
	}
	else {
	    byte [] new_bytes = new byte [20];
	    System.arraycopy (bytes, 0, new_bytes, 20 - bytes.length, 
		    bytes.length);
	    ib = new ByteArrayInputBuffer (new_bytes);
	}
	SHA1Hash result = new SHA1Hash (ib);
	return result;
    }

    protected static void check_bpd_and_dv (
            int bits_per_digit, int digit_values) {
        // assume that bits_per_digit() = 2^n for some positive
        // integer n <= 3
        if (bits_per_digit != 1 && bits_per_digit != 2 && 
            bits_per_digit != 4 && bits_per_digit != 8) {
            throw new IllegalArgumentException ("bits_per_digit != 2^n");
        }

        int i = 1;
        for (int j = 0; j < bits_per_digit; ++j)
            i <<= 1;
        if (i != digit_values)
            throw new IllegalArgumentException ("digit_value");
    }

    /**
     * Convert a big integer to an array of <code>digits_per_guid</code>
     * integers, one for each digit of the the big integer.  This function was
     * mostly cribbed from ostore.tapestry.impl.RoutingTable.digits, which I
     * think was originally written by Jeremy Stribling.
     */
    public static int [] guid_to_digits (BigInteger guid, 
            int bits_per_digit, int digits_per_guid, int digit_values) {

        check_bpd_and_dv (bits_per_digit, digit_values);

        byte [] buf = guid.toByteArray ();
        int digits_per_byte = 8/bits_per_digit;
        int space = buf.length - digits_per_guid / digits_per_byte;
        int [] result = new int [digits_per_guid]; // won't touch leading zeros
        int i = result.length - 1;
        while ((i >= 0) && (space + i/digits_per_byte >= 0)) {
            for (int j = i; j > (i-digits_per_byte); j--) {
                // System.out.println ("space=" + space);
                // System.out.println ("i=" + i);
                // System.out.println ("j=" + j);
                result [j] = ((int) (buf [space + i/digits_per_byte] >>> 
                            (i-j)*bits_per_digit) & 
                        (digit_values-1));
            }
            i -= digits_per_byte;
        }

        return result;
    }

    public static BigInteger digits_to_guid (int [] digits,
            int bits_per_digit, int digits_per_guid, int digit_values) {

        check_bpd_and_dv (bits_per_digit, digit_values);
        if (digits.length != digits_per_guid)
            throw new IllegalArgumentException ("digits [] wrong size");

        // Pad with a leading zero so always positive in 2's complement.
        int digits_per_byte = 8/bits_per_digit;
        byte [] buf = new byte [digits_per_guid / digits_per_byte + 1];
        for (int bnum = 0; bnum < buf.length - 1; ++bnum) {
            int bval = 0;
            for (int dnum = 0; dnum < digits_per_byte; ++dnum) {
                bval *= digit_values;
                bval |= digits [bnum*digits_per_byte+dnum];
            }
            buf [bnum+1] = (byte) bval;
        }
        return new BigInteger (buf);
    }

    public static BigInteger calc_dist (
            BigInteger a, BigInteger b, BigInteger mod) {
	BigInteger one = b.subtract (a).mod (mod);
	BigInteger two = a.subtract (b).mod (mod);
	if (one.compareTo (two) <= 0)
	    return one;
	else 
	    return two;
    }

    public static boolean in_range_mod (
	    BigInteger low, BigInteger high, BigInteger query, BigInteger mod) {

	if (low.compareTo (high) <= 0) {
	    // [low, high] does not wrap around.
	}
	else {
	    // [low, high] wraps; make it not.
	    high = high.add (mod);
	}

	if (low.compareTo (query) <= 0) {
	    // [low, query] does not wrap around.
	}
	else {
	    // [low, query] wraps; make it not.
	    query = query.add (mod);
	}

	boolean result = ((query.compareTo (high) <= 0) &&
		(query.compareTo (low) >= 0));

	/* System.err.println (
		"low=" + guid_to_string (low) +
		", high=" + guid_to_string (high) +
		", query=" + guid_to_string (query) +
		", result=" + result); */

	return result;
    }

    /**
     * A test harness for digits_to_guid and guid_to_digits.
     */
    public static void main (String [] args) {
        int bits_per_digit = Integer.parseInt (args [0]);
        int digits_per_guid = Integer.parseInt (args [1]);
        int digit_values = Integer.parseInt (args [2]);
        Random rand = new Random (1);

        for (int i = 0; i < 1000; ++i) {
            BigInteger n = random_guid (rand);
            int [] digits = guid_to_digits (n,
                    bits_per_digit, digits_per_guid, digit_values);
            BigInteger m = digits_to_guid (digits,
                    bits_per_digit, digits_per_guid, digit_values);
            if ((! m.equals (n)) || (digits.length != 40)) {
                System.err.println ("n=" + guid_to_string (n));
                System.err.print   ("d=");
                for (int j = 0; j < digits.length; ++j)
                    System.err.print (Integer.toHexString (digits [j]));
                System.err.println ();
                System.err.println ("m=" + guid_to_string (m));
                System.exit (1);
            }
        }
    }
}















