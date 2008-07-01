/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.fst;

/**
 * This class is top-level, rather than a subclass of bamboo.fst.Tree, because
 * as far as I can tell, there's no other way to restrict the set of
 * constructors.  Right now there is only one actual constructor, which just
 * takes a value for each of the fields, all of which are final.  There are
 * then several "named constructors" that build Nodes that obey certain
 * invariants:
 * <ul>
 * <li> All nodes have either 0 (leaves) or 2 (interior nodes) children.
 * <li> In a leaf: 
 *      <ul> 
 *      <li> high = low
 *      <li> value = 0
 *      <li> height = 1
 *      </ul>
 * <li> In an interior node: 
 *      <ul> 
 *      <li> low = left.low
 *      <li> high = right.high
 *      <li> value = max(left.offset+left.value,right.offset+right.value)
 *      <li> height = max(left.height, right.height)
 *      </ul>
 * <li> Neither value nor offset can be accessed in an invalid node.
 * </ul>
 */
public class Node {

    public final Node left;
    public final Node right;
    private final long offset;
    private final long value;
    public final long low;
    public final long high;
    public final int height;
    public final boolean valid;

    public final long offset() { assert valid; return offset; }
    public final long value() { assert valid; return value; }

    public final boolean leaf () { return left == null; }
    public final long time () { assert leaf (); return low; }
    public final long sum () { return value + offset; }

    public final boolean balanced () {
        return leaf () || (Math.abs (left.height - right.height) <= 1);
    }

    private Node(Node left, Node right, long offset, long value, long low,
            long high, int height, boolean valid) {
        this.left = left; this.right = right; this.offset = offset;
        this.value = value; this.low = low; this.high = high;
        this.height = height; this.valid = valid;
    }

    public static final Node makeLeaf(long offset, long time) {
        return new Node(null, null, offset, 0, time, time, 1, true);
    }

    public static final Node makeParent(long offset, Node left, Node right) {
        assert left != null;
        assert right != null;
        return new Node(left, right, offset, 
                        Math.max (left.valid ? left.value + left.offset : 0, 
                                  right.valid ? right.value + right.offset : 0),
                        left.low, right.high, 
                        1 + Math.max (left.height, right.height),
                        left.valid || right.valid);
    }

    public static final Node incrementOffset(Node old, long inc) {
        assert old != null;
        return new Node(old.left, old.right, old.offset + inc, old.value, 
                        old.low, old.high, old.height, old.valid);
    }

    public static final Node invalidate(Node old) {
        assert old != null;
        return new Node(old.left, old.right, old.offset, old.value, 
                        old.low, old.high, old.height, false);
    }
}

