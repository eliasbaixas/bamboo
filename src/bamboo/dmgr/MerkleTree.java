/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dmgr;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.log4j.Logger;

/**
 * A Merkle Tree over the keys stored on a Bamboo node.
 *
 * @author Sean C. Rhea
 * @version $Id: MerkleTree.java,v 1.25 2004/02/13 04:39:29 srhea Exp $
 */
public class MerkleTree {

    protected Logger logger;

    /**
     * Each node in this tree has at most 2^<code>expansion</code> children.
     */
    protected int expansion;

    /**
     * A array of maps that store the nodes in a given level of the tree.
     * The nodes in level <code>i</code> are stored in <code>nodes[i]</code>;
     * level is the level of internal nodes closest to the leaves, which are
     * the items of the database this Merkle tree covers.  Within the maps,
     * the nodes are indexed by the lowest timestamp value that they cover.
     * If a node has no children, it is not stored in this data structure.
     * (Otherwise, we'd need a lot of memory to store them all.)
     */
    protected Map [] nodes;

    /**
     * If you need a new digest, call <code>new_digest</code>; don't use this
     * variable.  <code>new_digest</code> just clones this digest, which is
     * always in the initialized state.  I expect this to be faster than doing
     * a call to new MessageDigest ("SHA") or whatever.
     */
    protected MessageDigest dont_use;

    /**
     * The data structure returned by <code>fill_holes</code>.
     */
    public static class FillHolesState {
        public byte [] digest;
	public long earliest_expiry_usec;
        public long leaves_below, max_leaves_below, range_low, range_high;
        public FillHolesState (long m, long l, long h) {
            max_leaves_below = m; range_low = l; range_high = h;
        }
        public String toString () {
            return "(FillHolesState digest=" + digest + ", range=[" + 
                Long.toHexString (range_low) + ", " + 
                Long.toHexString (range_high) + "), " + 
                ", leaves_below=" + leaves_below +
                ", max_leaves_below=" + max_leaves_below + ")";
        }
    }

    /**
     * A node in the tree.  Most tree operations are done on these objects.
     * To get the root node, call <code>MerkleTree.root ()</code>.
     */
    public class Node {

        /**
         * The lowest timestamp value that this node is a parent of.
         */
        protected long low;

        /**
         * This node's level in the tree; leaves are below level 0.
         */
        protected int level;

        /**
         * The hash of this node's children; may be null, in which case
         * <code>fill_holes</code> may be called to fill it in.
         */
        protected byte [] hash;

	/**
	 * The earliest expiration time in subtree below this node.
	 */
	protected long earliest_expiry_usec;

        /** 
         * A count of how many leaves are below this node, so that
         * <code>fill_holes</code> knows when we need to create or remove
         * child nodes.  If there are less than or equal to
         * 2^<code>expansion</code> leaves below this node, it'll have no
         * children, otherwise, it'll have at least two.
         */
        protected long leaves_below;

        /**
         * A class for iterating over this node's children.
         */
        public class Iter {

            long start;
            int position;

            public Iter (long s) { 
                start = s;  position = -1;
            }

            public boolean hasNext () { 
                return where (position+1) < range_high (); 
            }

            public Node next () { 
                ++position;
                long where = where (position);
                if (where >= range_high ())
                    throw new NoSuchElementException ();
                return (Node) nodes [level-1].get (new Long (where));
            }

            /**
             * Create a child node at the current iterator position.
             *
             * @throws NoSuchElementException if the
             * <code>hasNext()==false</code>.
             *
             * @throws IllegalStateException if there is already a child at
             * the current position.
             */
            public Node create () { 
                long where = where (position);
                if (where >= range_high ())
                    throw new NoSuchElementException ();
                if (nodes [level-1].get (new Long (where)) != null)
                    throw new IllegalStateException ();
                Node result = new Node (level - 1, where);
                nodes [level-1].put (new Long (where), result);
                return result;
            }

            public void remove () {
                long where = where (position);
                if (where >= range_high ())
                    throw new NoSuchElementException ();
                nodes [level-1].remove (new Long (where));
            }

            /**
             * Returns the lowest timestamp covered by the child at the 
             * current iterator position.
             */
            protected long where (int position) {
                return start + position*(1L<<((level-1)*expansion));
            }
        }

        public Node (int level, long low) {
            this.level = level; this.low = low;
        }

        /**
         * Returns the lowest timestamp covered by this node.
         */
        public long range_low () {
            return low;
        }

        /**
         * Returns the highest timestamp covered by this node.
         */
        public long range_high () {
            return low+(1L<<(level*expansion));
        }

        /**
         * Returns an iterator over this node's children.  Only valid for
         * nodes above level 0.  
         *
         * @throws UnsupportedOperationException if this node is at level 0
         */
        public Iter children () {
            if (level == 0)
                throw new UnsupportedOperationException ();
            return new Iter (low);
        }

        public boolean has_children () {
	    if (level == 0) 
		return false;
            for (Iter i = children (); i.hasNext (); ) {
                if (i.next () != null) 
                    return true;
            }
            return false;
        }

        /**
         * Notify this node that its current hash value should be recomputed,
         * presumably because of a change in the underlying database.
         */
        public void invalidate () {
            hash = null;
        }

        /**
         * Is the hash of this node up to date?
	 * Have any of this node's progeny expired?
         */
        public boolean valid (long now_ms) {
            return ((hash != null) &&
		    (earliest_expiry_usec >= (now_ms * 1000)));
        }

        /**
         * This node's level in the tree; leaves are below level 0.
         */
        public int level () {
            return level;
        }

        public boolean children_are_leaves () {
            if (hash == null)
                throw new IllegalStateException ("not valid");
            return (level == 0) || (! has_children ());
        }

        public long leaves_below () {
            if (hash == null)
                throw new IllegalStateException ("not valid");
            return leaves_below;
        }

        public void set_leaves_below (long value) {
            if (value < 0)
                throw new IllegalArgumentException ("value < 0");
            leaves_below = value;
        }

        public byte [] hash () {
            if (hash == null)
                throw new IllegalStateException ("not valid");
            return hash;
        }

	public long earliest_expiry_usec () {
	    return earliest_expiry_usec;
	}

        public void set_hash (byte [] value) {
            if (value == null)
                throw new IllegalArgumentException ("null");
            hash = value;
        }

	public void set_earliest_expiry_usec (long e_e) {
	    earliest_expiry_usec = e_e;
	}

        public String toString () {
            return "(Node level=" + level + ", range=[" + 
                Long.toHexString (range_low ()) + ", " + 
                Long.toHexString (range_high ()) + "), " + 
                (hash == null ? "invalid" :
                 ("hash=" + ostore.util.ByteUtils.print_bytes (hash, 0, 3))) + 
                ", leaves_below=" + leaves_below + ")";
        }

        /**
         * Invalidate all nodes between this one and the one which has (or
         * would have) timestamp <code>time_usec</code> as its leaf.
         */
        public void invalidate_path (long time_usec) {
            if (logger.isDebugEnabled ()) 
                logger.debug ("invalidate_path " + this);
            invalidate ();
            if (level > 0) {
                for (Iter i = children (); i.hasNext (); ) {
                    Node child = i.next ();
                    if ((child != null) &&
                            (time_usec >= child.range_low ()) &&
                            (time_usec <= child.range_high ())) {
                        child.invalidate_path (time_usec);
                        break;
                    }
                }
            }
        }

        /**
         * Fills in the holes in the tree.  If all of the children of this
         * node are valid, this function will compute the <code>hash</code>
         * and <code>leaves_below</code> data members for this node correctly
         * and return <code>null</code>.  Otherwise, it will discover a range
         * of data for which it needs a hash and a count of the number of
         * items before it can complete successfully.  In this case, it will
         * return a <code>FillHoleState</code> to indicate the range it needs.
         * The caller should perform the scan over the range
         * <code>range_low</code> to <code>range_high</code> inclusive. If
         * there are less than or equal to <code>max_leaves_below</code> items
         * in the range, it should set <code>leaves_below</code> to the total
         * number of items and store their digest in <code>digest</code>.
         * Otherwise, it should set <code>leaves_below</code> to the number of
         * items read and set <code>digest</code> to <code>null</code>.  It
         * should not change <code>range_low</code>, <code>range_high</code>,
         * or <code>max_leaves_below</code>.
         */
        public FillHolesState fill_holes (FillHolesState state, long now_ms) {

            if (logger.isDebugEnabled ()) 
                logger.debug ("fill_holes " + toString ());
            if (logger.isDebugEnabled ()) logger.debug ("state=" + state);

            if (valid (now_ms)) {
                if (logger.isDebugEnabled ()) logger.debug ("valid");
                return null;
            }

            FillHolesState result = has_children ()
                ? fill_holes_have_children (state, now_ms)
                : fill_holes_no_children (state, now_ms);

            // either we're valid or we need more information
            assert valid (now_ms) || (result != null) 
                : "not valid at end of fill_holes";

            return result;
        }

        protected FillHolesState fill_holes_have_children (
                FillHolesState state, long now_ms) {

            if (logger.isDebugEnabled ()) logger.debug ("checking children");
            if (logger.isDebugEnabled ()) logger.debug ("state=" + state);

            // Make sure all our children are valid, compute our hash, and
            // find out how many leaves are below us.
            int leaves_below = 0;
	    long earliest_expiry_usec = Long.MAX_VALUE;
            MessageDigest md = new_digest ();
            for (Iter i = children (); i.hasNext (); ) {
                Node child = i.next ();
                if (! child.valid (now_ms)) {
                    FillHolesState result = child.fill_holes (state, now_ms);
                    if (result != null)
                        return result;
                }
                md.update (child.hash ());
                leaves_below += child.leaves_below ();
		if (child.earliest_expiry_usec() < earliest_expiry_usec)
		    earliest_expiry_usec = child.earliest_expiry_usec();
            }

            if (leaves_below <= max ()) {
                // Collapse all of our children into us.
                for (Iter i = children (); i.hasNext (); ) {
                    i.next (); i.remove ();
                }
                return fill_holes_no_children (state, now_ms);
            }
            else {
                // If we get here, all of our children were valid and there
                // are too many leaves to collapse them all into us, so we're
                // done.
                set_hash (md.digest ());
		set_earliest_expiry_usec(earliest_expiry_usec);
                set_leaves_below (leaves_below);
                assert valid (now_ms) 
                    : "not valid at end of fill_holes_have_children";
                return null;
            }
        }

        protected long max () {
            return (level == 0) ? Long.MAX_VALUE : (1<<expansion);
        }

        protected FillHolesState fill_holes_no_children (
                FillHolesState state, long now_ms) {

            if (logger.isDebugEnabled ()) logger.debug ("no children");
            if (logger.isDebugEnabled ()) logger.debug ("state=" + state);

            // See if the scan that was passed in is sufficient to complete
            // this node's hash.

            if (state == null) {

                if (logger.isDebugEnabled ()) logger.debug ("need a scan");

                return new FillHolesState (max (), range_low (), range_high ());
            }

            if ((state.range_low != range_low ()) ||
                (state.range_high != range_high ())) {

                if (logger.isDebugEnabled ()) 
                    logger.debug ("bad range: wanted ["
                            + Long.toHexString (range_low ()) + ", "
                            + Long.toHexString (range_high ()) + "] but got [" 
                            + Long.toHexString (state.range_low) + ", " 
                            + Long.toHexString (state.range_high) + "]");

                return new FillHolesState (max (), range_low (), range_high ());
            }

            if (state.digest != null) {

                if (state.earliest_expiry_usec < (now_ms*1000)) {

                    // We have a good scan, but it has expired since the
                    // digest was computed.  Try again.

                    return new FillHolesState (
                            max (), range_low (), range_high ());
                }

                // We have a good scan.

                assert state.leaves_below <= max () : "too many leaves";

                if (logger.isDebugEnabled ()) logger.debug ("good scan");

                set_hash (state.digest);
		set_earliest_expiry_usec(state.earliest_expiry_usec);
                set_leaves_below (state.leaves_below);
                assert valid (now_ms) 
                    : "not valid at end of fill_holes_no_children";
                return null;
            }

            if (state.leaves_below <= max ())
                throw new IllegalArgumentException (
                        "leaves_below <= max: why isn't digest filled in?");

            // We got a good scan, but there were too many keys.
            // Throw it away, create our children, and recurse.

            if (logger.isDebugEnabled ()) logger.debug ("creating children");
            for (Iter i = children (); i.hasNext (); ) {
                i.next (); i.create ();
            }

            return fill_holes_have_children (null, now_ms);
        }
    }

    /**
     * Create a new MerkleTree with 2^<code>exp</code> children per node and
     * using the algorithm of <code>md</code> to compute digests.  Note that
     * a clone of <code>md</code> will be used rather than <code>md</code>
     * itself.
     */
    public MerkleTree (int exp, MessageDigest md) {

        logger = Logger.getLogger (getClass ());

        try {
            dont_use = (MessageDigest) md.clone ();
        }
        catch (CloneNotSupportedException e) {
            throw new IllegalArgumentException ("md must support clone");
        }

        expansion = exp;
        int levels = (62 / expansion) + 1;
        nodes = new Map [levels];
        for (int i = 0; i < nodes.length; ++i)
            nodes [i] = new HashMap ();
    }

    public Node root () {
        Long l = new Long (0L);
        int level = nodes.length - 1;
        Node result = (Node) nodes [level].get (l);
        if (result == null) {
            result = new Node (level, l.longValue ());
            nodes [level].put (l, result);
        }
        return result;
    }

    public Node node (int level, long low) {
        if (level >= nodes.length)
            return null;
        return (Node) nodes [level].get (new Long (low));
    }

    protected MessageDigest new_digest () {
        try {
            return (MessageDigest) dont_use.clone ();
        }
        catch (CloneNotSupportedException e) {
            assert false;
            return null; // unreachable
        }
    }
}

