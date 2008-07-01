/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.util;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * Gets the cycle counter on Linux machines; to be used in situations where
 * System.currentTimeMillis is no good because of NTP constantly resetting the
 * clock, sometimes back in time (as on PlanetLab).  Uses a C library written
 * by Aki Nakao and available
 * <a href="http://www.cs.princeton.edu/~nakao/gettimeofday_cc.tgz>here</a>.
 *
 * @author  Sean C. Rhea
 * @version $Id: GetTimeOfDayCC.java,v 1.1 2004/05/03 17:55:43 srhea Exp $
 */
public class GetTimeOfDayCC {

    protected static Logger logger = Logger.getLogger (GetTimeOfDayCC.class);

    /**
     * Returns true if its safe to call GetTimeOfDayCC.currentTimeMillis.
     */
    public static boolean available () { return loaded; }
    private static boolean loaded = false;
    static {
        try {
            System.loadLibrary ("GetTimeOfDayCC");
            loaded = true;
        }
        catch (Exception e) {
            logger.warn ("GetTimeOfDayCC not available");
            logger.debug (e);
        }
        catch (Error e) {
            logger.warn ("GetTimeOfDayCC not available");
            logger.debug (e);
        }
    }

    public static native long currentTimeMillis ();

    public static void main (String [] args) throws Exception {
        PatternLayout pl = new PatternLayout ("%d{ISO8601} %-5p %c: %m\n");
        ConsoleAppender ca = new ConsoleAppender (pl);
        Logger.getRoot ().addAppender (ca);
        Logger.getRoot ().setLevel (Level.INFO);

        if (available ()) {
            long now = currentTimeMillis ();
            Thread.currentThread ().sleep (1000);
            long delay = currentTimeMillis () - now;
            logger.info ("delay=" + delay);
        }
    }
}

