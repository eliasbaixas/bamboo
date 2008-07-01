/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.util;
import java.text.NumberFormat;
import java.net.InetSocketAddress;

public class StringUtil {

    //////////////////////////////////////////////////////////////////////////
    //
    // Useful string-formatting routines.
    //
    //////////////////////////////////////////////////////////////////////////

    public static void addr_to_sbuf(InetSocketAddress addr, StringBuffer buf) {
        buf.append(addr.getAddress().getHostAddress());
        buf.append(":");
        buf.append(addr.getPort());
    }

    private static final char[] digits = {
	'0' , '1' , '2' , '3' , '4' , '5' , '6' , '7' , '8' , '9' ,
	'a' , 'b' , 'c' , 'd' , 'e' , 'f'
    };

    public static String bytes_to_str (byte [] data) {
        return bytes_to_str (data, 0, data.length, false);
    }

    public static String bytes_to_str (byte [] data, int offset, int length) {
        return bytes_to_str(data, offset, length, false);
    }

    public static String bytes_to_str (byte [] data, int offset, int length, 
                                       boolean pretty) {
        StringBuffer sbuf = new StringBuffer (length*2);
        bytes_to_sbuf (data, offset, length, pretty, sbuf);
        return sbuf.toString ();
    }

    public static void bytes_to_sbuf (byte [] data, int offset, int length,
                                      StringBuffer buf) {
        bytes_to_sbuf(data, offset, length, false, buf);
    }

    public static void bytes_to_sbuf (byte [] data, int offset, int length,
                                      boolean pretty, StringBuffer buf)
    {
        int size = 2 * length;
	size += (size/8) + (size/64);
	int low_mask = 0x0f;
	int high_mask = 0xf0;
	byte b;
	
	int j = 0;
	for (int i=offset; i < (offset+length); ++i) {
	    b = data[i];
	    buf.append (digits[(high_mask & b) >> 4]);
	    buf.append (digits[(low_mask & b)]);
            if (pretty) {
                if (j % 4 == 3)
                    buf.append (' ');
                if (j % 32 == 31)
                    buf.append ('\n');
            }
	    ++j;
	}
    }

    private static final int [] time_units = {
        1, 60, 3600, 3600*24, 3600*24*7
    };
    private static final String [] time_desc = {
        "second", "minute", "hour", "day", "week"
    };

    public static final void time_to_sbuf (long secs, StringBuffer buf) {
        int i = time_units.length - 1;
        while (secs > 0) {
            if (secs >= time_units [i]) {
                long div = secs/(time_units [i]);
                secs %= time_units [i];
                buf.append(div);
                buf.append(" ");
                buf.append(time_desc [i]);
                if (div != 1)
                    buf.append("s");
                if (secs > 0)
                    buf.append (" ");
            }
            --i;
        }
    }

    private static final long [] byte_units = {
        1, 1024, 1024*1024, 1024*1024*1024
    };
    private static final String [] byte_desc = { "byte", "KB", "MB", "GB" };

    public static final void byte_cnt_to_sbuf (long bytes, StringBuffer buf) {
        int i = byte_units.length - 1;
        while (true) {
            if (i == 0 || bytes >= byte_units [i]) {
                double result = ((double) bytes)/(byte_units [i]);
                NumberFormat n = NumberFormat.getInstance ();
                n.setMaximumFractionDigits(2);
                n.setGroupingUsed (false);
                buf.append(n.format(result));
                buf.append(" ");
                buf.append(byte_desc [i]);
                if (result != 1.0)
                    buf.append("s");
                break;
            }
            --i;
        }
    }
}

