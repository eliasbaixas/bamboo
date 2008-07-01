/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.util;
import java.util.Random;

public class RandomUtil {

    protected static double random_exponential(double mean, Random rand) {
        double u = rand.nextDouble();
        return (0 - (mean * Math.log(1.0 - u)));
    }

    protected static double random_gaussian (double mean, double stddev,
            Random rand) {
        // Taken from http://www.taygeta.com/random/gaussian.html
        double x1, x2, w, y1, y2;
        do {
            x1 = 2.0 * rand.nextDouble () - 1.0;
            x2 = 2.0 * rand.nextDouble () - 1.0;
            w = x1 * x1 + x2 * x2;
        } while ( w >= 1.0 );
        w = Math.sqrt( (-2.0 * Math.log( w ) ) / w );
        y1 = x1 * w;
        // y2 = x2 * w;
        return mean + y1 * stddev;
    }
}
