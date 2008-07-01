/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht;

public class IonTree {

    protected static class Node {
        public final Node left, right;
        public final long offset, value;
        public final long low, high;
        public final int height;
        public final boolean leaf () { return left == null; }
        public final long time () { assert leaf (); return low; }
        public final long sum () { return value + offset; }
        public final boolean balanced () {
            return leaf () || (Math.abs (left.height - right.height) <= 1);
        }

        public Node (long o, long t) {
            left = null; right = null; height = 1;
            offset = o; value = 0; low = high = t;
        }
        public Node (long o, Node l, Node r) {
            assert l != null;
            assert r != null;
            left = l; right = r; offset = o;
            value = Math.max (l.value + l.offset, r.value + r.offset);
            low = l.low; high = r.high;
            height = 1 + Math.max (l.height, r.height);
        }
        public Node (Node old, long inc) {
            assert old != null;
            left = old.left; right = old.right; 
            offset = old.offset + inc; value = old.value; 
            low = old.low; high = old.high; height = old.height;
        }
    }

    protected static String values_to_string (Node n, long offset) {
        if (n.leaf ()) {
            return n.time () + " " + (n.offset + offset) + "\n";
        }
        else {
            return values_to_string (n.left,  n.offset + offset) +
                   values_to_string (n.right, n.offset + offset);
        }
    }

    protected static String values_to_string (Node n) {
        if (n == null)
            return "";
        else
            return values_to_string (n, 0);
    }

    protected static String node_to_string (Node n) {
        return node_to_string (n, "");
    }

    protected static String node_to_string (Node n, String indent) {
        if (n == null)
            return "(empty tree)";
        String local = indent + "[" + n.low + ", " + n.high + "] off=" + 
            n.offset + " val=" + n.value + " height=" + n.height;
        if (n.leaf ())
            return local;
        else {
            return local + "\n" + node_to_string (n.left, indent + "  ") +
                "\n" + node_to_string (n.right, indent+"  ");
        }
    }

    protected static Node rotate_right (Node n) {
        return new Node (n.offset, 
                new Node (n.left.left, n.left.offset), 
                new Node (0, new Node (n.left.right, n.left.offset), n.right));
    }

    protected static Node rotate_left (Node n) {
        return new Node (n.offset, 
                new Node (0, n.left, new Node (n.right.left, n.right.offset)),
                new Node (n.right.right, n.right.offset));
    }

    protected static Node balance (Node n) {
        if ((n == null) || n.leaf () || n.balanced ()) 
            return n;
        Node result = null;
        if (n.left.height > n.right.height) {
            if (n.left.left.height > n.left.right.height) {
                result = rotate_right (n);
            }
            else {
                result = rotate_right (
                        new Node (n.offset, rotate_left (n.left), n.right));
            }
        }
        else {
            if (n.right.right.height > n.right.left.height) {
                result = rotate_left (n);
            }
            else {
                result = rotate_left (
                        new Node (n.offset, n.left, rotate_right (n.right)));
            }
        }
        assert result.balanced ();
        return result;
    }

    protected static Node create_point (Node n, long rate, long time) {
        // System.out.println ("create_point:");
        // System.out.println (node_to_string (n));
        Node result = null;
        if (n.leaf ()) {
            if (n.time () == time) {
                result = n;
            }
            else if (time < n.time ()) {
                Node l = new Node (n.sum() - (n.time() - time) * rate, time);
                result = new Node (0, l, n);
            }
            else {
                Node r = new Node (n.sum() + (time - n.time()) * rate, time);
                result = new Node (0, n, r);
            }
        }
        else {
            if (time <= n.left.high) {
                Node left = balance (create_point (n.left, rate, time));
                Node parent = new Node (n.offset, left, n.right);
                result = balance (parent);
            }
            else {
                Node right = balance (create_point (n.right, rate, time));
                Node parent = new Node (n.offset, n.left, right);
                result = balance (parent);
            }
        }
        // System.out.println ("create_point result:");
        // System.out.println (node_to_string (result));
        return result;
    }

    protected static Node increment_range (
            Node n, long low, long high, long inc) {
        // If completely covered, increment only this node and return.
        if ((low <= n.low) && (high >= n.high))
            return new Node (n, inc);
        // If not covered at all, don't do anything.
        if ((low > n.high) || (high < n.low))
            return n;
        return new Node (n.offset, 
                         increment_range (n.left, low, high, inc),
                         increment_range (n.right, low, high, inc));
    }

    protected static Node remove_range (Node n, long low, long high) {
        if ((high < n.low) || (low > n.high))
            return n;
        if ((low <= n.low) && (high >= n.high))
            return null;
        Node l = remove_range (n.left, low, high);
        Node r = remove_range (n.right, low, high);
        if (l == null)
            return new Node (r, n.offset);
        if (r == null)
            return new Node (l, n.offset); 
        Node p = new Node (n.offset, l, r);
        return balance (p);
    }

    protected static Node add_put (
            Node n, long now, long rate, long ttl, long size) {
        if (n == null) {
            Node a = new Node (size, now);
            Node b = new Node (size + rate * (ttl - 1), now + ttl - 1);
            Node c = new Node (0, a, b);
            Node d = new Node (rate * ttl, now + ttl);
            Node e = new Node (0, c, d);
            return e;
        }

        n = create_point (n, rate, now);
        n = create_point (n, rate, now + ttl - 1);
        n = create_point (n, rate, now + ttl);
        return increment_range (n, now, now + ttl - 1, size);
    }

    protected static Node shift_time (Node n, long now, long rate, long then) {
        if (n == null)
            return n;

        // System.out.println ("shift_time:");
        // System.out.println (node_to_string (n));
        n = create_point (n, rate, then);
        // System.out.println (node_to_string (n));
        n = remove_range (n, now, then - 1);
        // System.out.println (node_to_string (n));
        n = increment_range (n, then, Long.MAX_VALUE, -1 * (then - now) * rate);
        // System.out.println (node_to_string (n));
        if ((n.leaf ()) && (n.value == 0) && (n.offset == 0))
            return null;
        else 
            return n;
    }

    protected long now, rate, capacity; 
    protected Node root;

    public IonTree (long n, long r, long c) {
        now = n; rate = r; capacity = c;
    }

    public void shift_time (long then) {
        root = shift_time (root, now, rate, then);
        now = then;
    }

    public boolean accept_put (long ttl, long size) {
        // System.out.println ("ttl=" + ttl + " size=" + size);
        // System.out.println (node_to_string (root));
        Node new_root = add_put (root, now, rate, ttl, size);
        // System.out.println (node_to_string (new_root));
        return new_root.value + new_root.offset <= capacity;
    }

    public boolean add_put (long ttl, long size) {
        Node new_root = add_put (root, now, rate, ttl, size);
        if (new_root.value + new_root.offset <= capacity) {
            root = new_root;
            return true;
        }
        else {
            return false;
        }
    }

    public String toString () {
        return node_to_string (root);
    }

    public static void main (String [] args) {
        long ttl = 4; long put = 1;
        IonTree t = new IonTree (1, put, put * ttl);
        t.add_put (4, put);
        System.out.println (node_to_string (t.root));
        t.add_put (2, put);
        System.out.println (node_to_string (t.root));
        t.add_put (3, put);
        System.out.println (node_to_string (t.root));
        t.add_put (1, put);
        System.out.println (node_to_string (t.root));
        t.shift_time (2);
        System.out.println (node_to_string (t.root));
        t.add_put (4, put);
        System.out.println (node_to_string (t.root));
        t.shift_time (3);
        System.out.println (node_to_string (t.root));
        t.add_put (4, put);
        System.out.println (node_to_string (t.root));
     }
}

