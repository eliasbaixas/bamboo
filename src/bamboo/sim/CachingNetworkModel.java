/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.sim;
import bamboo.util.LruMap;
import java.util.HashMap;

public abstract class CachingNetworkModel implements NetworkModel {

    public static class SrcDst {
        public int src, dst;
        public SrcDst (int s, int d) { src = s; dst = d; }
        public boolean equals (Object rhs) {
            SrcDst other = (SrcDst) rhs;
            return (src == other.src) && (dst == other.dst);
        }
        public int hashCode () { return (src << 16) | (dst & 0xffff); }
    }

    public CachingNetworkModel (int cache_size) {
        cache = new LruMap (cache_size, new HashMap ());
    }

    public synchronized RouteInfo compute_route_info (int src, int dst) {
        SrcDst key = new SrcDst (src, dst);
        RouteInfo cached = (RouteInfo) cache.get (key);
        if (cached == null) {
            cached = the_real_compute_route_info (src, dst);
            cache.put (key, cached);
        }
        return cached;
    }

    protected abstract RouteInfo the_real_compute_route_info (int src, int dst);

    protected LruMap cache;
}

