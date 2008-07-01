/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.sim;

public interface NetworkModel {

    public static class RouteInfo {
	public RouteInfo (long l, double b) { 
	    latency_us = l; inv_bw_us_per_byte = b; 
	}
	public long latency_us;
	public double inv_bw_us_per_byte;
    }

    public RouteInfo compute_route_info (int src, int dst);
}

