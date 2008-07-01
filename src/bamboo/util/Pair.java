/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.util;

public class Pair<T1,T2> {

    public T1 first;
    public T2 second;

    /**
     * Use this constructor if you're masochistic; otherwise, use {@link
     * #create}. 
     */
    public Pair (T1 f, T2 s) { first = f; second = s; }

    /**
     * Much nicer to use than new, as you don't have to specify the argument
     * types explicitly
     */
    public static <T1,T2> Pair<T1,T2> create(T1 t1, T2 t2) { 
        return new Pair(t1, t2);
    }

    public int hashCode() {
        if (first == null && second == null)
            return 0;
        if (first == null)
            return second.hashCode();
        if (second == null)
            return first.hashCode();
        return first.hashCode() ^ second.hashCode();
    }

    public boolean equals(Object other) {
	Pair rhs = (Pair) other;
        if (first == null && second == null)
            return rhs.first == null && rhs.second == null;
        if (first == null)
            return rhs.first == null && second.equals(rhs.second);
        if (second == null)
            return rhs.second == null && first.equals(rhs.first);
        return first.equals(rhs.first) && second.equals(rhs.second);
    }
}

