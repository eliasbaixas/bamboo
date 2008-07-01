/*
 * Copyright (c) 2004 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.util;

public class USecTimer {

    static {
        try {
            System.loadLibrary ("USecTimer");
        }
        catch (Exception e) {
            System.err.println ("Error loading libUSecTimer");
            e.printStackTrace (System.err);
            System.exit (1);
        }
    }

    public static native long currentTimeMicros();
}
