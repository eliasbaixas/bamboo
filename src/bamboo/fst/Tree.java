/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.fst;

import java.awt.*;
import java.awt.event.*;
import java.awt.geom.*;
import javax.swing.*;
import static bamboo.fst.Node.*;

public class Tree {

    public static String nodeToString(Node n) {
        return nodeToString(n, "");
    }

    protected static String nodeToString(Node n, String indent) {
        if (n == null)
            return "(empty tree)";
        String local = indent + "[" + n.low + ", " + n.high 
            + "] height=" + n.height;
        if (n.valid)
            local += " offset=" + n.offset() + " value=" + n.value();
        else 
            local += " (invalid)";
        if (n.leaf ())
            return local;
        else {
            return local + "\n" + nodeToString(n.left, indent + "  ") +
                "\n" + nodeToString(n.right, indent+"  ");
        }
    }

    public static Node rotateRight(Node n) {
        return makeParent(n.offset(), 
                incrementOffset(n.left.left, n.left.offset()), 
                makeParent(0, incrementOffset(n.left.right, n.left.offset()),
                           n.right));
    }

    public static Node rotateLeft(Node n) {
        return makeParent(n.offset(), 
                makeParent(0, n.left, 
                           incrementOffset(n.right.left, n.right.offset())),
                incrementOffset(n.right.right, n.right.offset()));
    }

    protected static Node balance (Node n) {
        if ((n == null) || n.leaf () || n.balanced ()) 
            return n;
        Node result = null;
        if (n.left.height > n.right.height) {
            if (n.left.left.height > n.left.right.height) {
                result = rotateRight(n);
            }
            else {
                result = rotateRight(
                        makeParent(n.offset(), rotateLeft(n.left), n.right));
            }
        }
        else {
            if (n.right.right.height > n.right.left.height) {
                result = rotateLeft(n);
            }
            else {
                result = rotateLeft(
                        makeParent(n.offset(), n.left, rotateRight(n.right)));
            }
        }
        assert result.balanced ();
        return result;
    }

    protected static Node createPoint(Node n, long rate, long time) {
        // System.out.println ("createPoint:");
        // System.out.println (nodeToString(n));
        Node result = null;
        if (n.leaf ()) {
            if (n.time () == time) {
                result = n;
            }
            else if (time < n.time ()) {
                Node l = makeLeaf(n.sum() - (n.time() - time) * rate, time);
                result = makeParent(0, l, n);
            }
            else {
                Node r = makeLeaf(n.sum() + (time - n.time()) * rate, time);
                result = makeParent(0, n, r);
            }
        }
        else {
            if (time <= n.left.high) {
                Node left = balance (createPoint(n.left, rate, time));
                Node parent = makeParent(n.offset(), left, n.right);
                result = balance (parent);
            }
            else {
                Node right = balance (createPoint(n.right, rate, time));
                Node parent = makeParent(n.offset(), n.left, right);
                result = balance (parent);
            }
        }
        // System.out.println ("createPoint result:");
        // System.out.println (nodeToString(result));
        return result;
    }

    public static Node incrementRange(Node n, long low, long high, long inc) {
        // If not covered at all, don't do anything.
        if ((low > n.high) || (high < n.low))
            return n;
        // Otherwise, we're going to change it, so it better be valid.
        assert n.valid : nodeToString(n);
        // If completely covered, increment only this node and return.
        if ((low <= n.low) && (high >= n.high))
            return incrementOffset(n, inc);
        else
            return makeParent(n.offset(), 
                              incrementRange(n.left, low, high, inc),
                              incrementRange(n.right, low, high, inc));
    }

    public static Node invalidateRange(Node n, long low, long high) {
        if ((high < n.low) || (low > n.high)) {
            // The given range is either entirely before or entirely after
            // this node, so don't change anything.
            return n;
        }
        if ((low <= n.low) && (high >= n.high)) {
            // The given range completely covers this node, so invalidate it.
            return invalidate(n);
        }
        return makeParent(n.offset(), 
                          invalidateRange(n.left, low, high),
                          invalidateRange(n.right, low, high));
    }

    public static Node addPut(
            Node n, long now, long rate, long ttl, long size) {
        if (n == null) {
            Node a = makeLeaf(size, now);
            Node b = makeLeaf(size + rate * (ttl - 1), now + ttl - 1);
            Node c = makeParent(0, a, b);
            Node d = makeLeaf(rate * ttl, now + ttl);
            Node e = makeParent(0, c, d);
            return e;
        }

        n = createPoint(n, rate, now);
        n = createPoint(n, rate, now + ttl - 1);
        n = createPoint(n, rate, now + ttl);
        return incrementRange(n, now, now + ttl - 1, size);
    }

    public static Node shiftTime(Node n, long now, long rate, long then) {
        if (n == null)
            return n;

        // System.out.println ("shiftTime:");
        // System.out.println (nodeToString(n));
        n = createPoint(n, rate, then);
        // System.out.println (nodeToString(n));
        n = invalidateRange(n, now, then - 1);
        // System.out.println (nodeToString(n));
        n = incrementRange(n, then, Long.MAX_VALUE, -1 * (then - now) * rate);
        // System.out.println (nodeToString(n));
        if ((n.leaf ()) && (n.value() == 0) && (n.offset() == 0))
            return null;
        else 
            return n;
    }

    protected long now, rate, capacity; 
    protected Node root;

    public Tree (long n, long r, long c) {
        now = n; rate = r; capacity = c;
    }

    public Tree (Tree other) {
        now = other.now; 
        rate = other.rate; 
        capacity = other.capacity; 
        root = other.root;
    }

    public long nextAccept(long ttl, long size) {
        if (acceptPut(ttl, size))
            return now;

        // It can't take any longer than size/rate to accept the put.
        long high = now + (size - 1) / rate + 1;

        // Make sure we can accept at time "high".
        {
            Node n = shiftTime(root, now, rate, high);
            n = addPut(n, high, rate, ttl, size);
            assert n.value() + n.offset() <= capacity : 
                   "\n" + nodeToString(n) + "\ncap=" + capacity 
                   + " now=" + now;
        }

        // Binary search between the good time and the time before it.
        long low = now;
        while (high - low > 1) {
            long then = low + (high - low) / 2;
            Node n = shiftTime(root, now, rate, then);
            n = addPut(n, then, rate, ttl, size);
            if (n.value() + n.offset() <= capacity)
                high = then;
            else 
                low = then;
        }

        // At time high, we can accept the put, and at time low, we can't.

        // System.out.println ("nextAccept: returning " + high);
        return high;
    }

    public int height() {
        return (root == null) ? 0 : root.height;
    }

    public void shiftTime(long then) {
        root = shiftTime(root, now, rate, then);
        now = then;
        // If the left subtree is all invalid, remove it.
        while ((root != null) && (!root.left.valid))
            root = incrementOffset(root.right, root.offset());
    }

    public boolean acceptPut(long ttl, long size) {
        // System.out.println ("ttl=" + ttl + " size=" + size);
        // System.out.println (nodeToString(root));
        Node newRoot = addPut(root, now, rate, ttl, size);
        // System.out.println (nodeToString(newRoot));
        return newRoot.value() + newRoot.offset() <= capacity;
    }

    public boolean addPut(long ttl, long size) {
        Node newRoot = addPut(root, now, rate, ttl, size);
        if (newRoot.value() + newRoot.offset() <= capacity) {
            root = newRoot;
            return true;
        }
        else {
            return false;
        }
    }

    public String toString () {
        return nodeToString(root);
    }

    public static class PointList {
        public long x, y;
        public PointList next;
        public PointList (long x, long y, PointList next) {
            this.x = x; this.y = y; this.next = next;
        }
        public PointList (long x, long y) { this (x, y, null); }
        public int size () {
            int result = 1; 
            for (PointList i = next; i != null; i = i.next) 
                ++result;
            return result;
        }
    }

    public String leavesToString(Node node, long aoff) {
        String result = "";
        PointList [] pl = leavesToPoints(node, aoff);
        for (PointList i = pl [0]; i != null; i = i.next) 
            result += "(" + i.x + ", " + i.y + ") ";
        return result;
    }

    public PointList[] leavesToPoints(Node node, long aoff) {
        if (node.leaf ()) {
            long val = node.valid 
                ? node.offset() + node.value() + aoff - rate *(node.low - now) 
                : 0;
            PointList left = new PointList (node.low, val);
            PointList right = new PointList (node.low + 1, val);
            left.next = right;
            return new PointList [] {left, right};
        }
        else {
            PointList [] left  = 
                leavesToPoints(node.left,  aoff + node.offset());
            PointList [] right = 
                leavesToPoints(node.right, aoff + node.offset());
            left [1].next = right [0];
            return new PointList [] {left [0], right [1]};
        }
    }

    public PointList[] valuesToPoints(Node node, long aoff) {
        if (node.leaf ()) {
            long val = node.valid ? node.offset() + node.value() + aoff : 0;
            PointList left = new PointList (node.low, val);
            PointList right = new PointList (node.low + 1, val);
            left.next = right;
            return new PointList [] {left, right};
        }
        else {
            PointList [] left  = 
                valuesToPoints(node.left,  aoff + node.offset());
            PointList [] right = 
                valuesToPoints(node.right, aoff + node.offset());
            left [1].next = right [0];
            return new PointList [] {left [0], right [1]};
        }
    }

    public String valuesToString(Node node, long aoff) {
        String result = "";
        PointList [] pl = valuesToPoints(node, aoff);
        for (PointList i = pl [0]; i != null; i = i.next) 
            result += "(" + i.x + ", " + i.y + ") ";
        return result;
    }

    public int scale = 30;
    public class Visual extends JApplet {
        public void init () {
            setBackground (Color.white);
            setForeground (Color.black);
        }

        public void paint(Graphics g) {
            if (root == null)
                return;

            Graphics2D g2 = (Graphics2D) g;
            Dimension d = getSize ();

            g2.setStroke (new BasicStroke(2.0f));

            PointList [] pl = leavesToPoints(root, 0);
            GeneralPath putsline = new GeneralPath (GeneralPath.WIND_EVEN_ODD,
                                                    pl [0].size ());
            GeneralPath rline = new GeneralPath (GeneralPath.WIND_EVEN_ODD);

            float px = (float) pl [0].x * scale;
            float py = d.height - (float) pl [0].y * scale;
            putsline.moveTo (px, py);
            for (PointList i = pl [0].next; i != null; i = i.next) {
                px = (float) i.x * scale;
                py = d.height - (float) i.y * scale;
                putsline.lineTo (px, py);
            }

            float rx = (float) now * scale;
            float ry = d.height; 
            rline.moveTo (rx, ry);
            while (ry > d.height - capacity * scale) { 
                rx += scale;
                rline.lineTo (rx, ry);
                ry -= rate * scale;
                rline.lineTo (rx, ry);
            }
            rx += scale;
            rline.lineTo (rx, ry);

            /*
            g2.draw (new Line2D.Double (0, d.height, 
                                        capacity / rate * scale, 
                                        d.height - capacity * scale));
            */

            pl = valuesToPoints(root, 0);
            GeneralPath sumline = 
                new GeneralPath (GeneralPath.WIND_EVEN_ODD, pl [0].size ());
            float sx = (float) pl [0].x * scale;
            float sy = d.height - (float) pl [0].y * scale;
            sumline.moveTo (sx, sy);
            for (PointList i = pl [0].next; i != null; i = i.next) {
                sx = (float) i.x * scale;
                sy = d.height - (float) i.y * scale;
                sumline.lineTo (sx, sy);
            }

            g2.setPaint (Color.black);
            g2.draw (putsline);
            g2.setPaint (Color.red);
            g2.draw (rline);
            g2.setPaint (Color.blue);
            g2.draw (sumline);


        }
    }

    public JFrame visualize () {
        JFrame f = new JFrame("Tree Visualization");
        JApplet applet = new Visual ();
        f.getContentPane().add("Center", applet);
        applet.init();
        f.pack();
        f.setSize(new Dimension(300,220));
        f.show();
        return f;
    }

    public static void main (String [] args) {
        long ttl = 4; long put = 1;
        Tree t = new Tree (0, put, put * ttl);
        System.out.println (nodeToString(t.root));
        ////////////////////////////////////////////////////
        t.addPut(4, put);
        System.out.println (nodeToString(t.root));
        JFrame f = t.visualize ();
        try { System.in.read (); } catch (Exception e) {}
        f.dispose ();
        ////////////////////////////////////////////////////
        t.addPut(2, put);
        System.out.println (nodeToString(t.root));
        f = t.visualize ();
        try { System.in.read (); } catch (Exception e) {}
        f.dispose ();
        ////////////////////////////////////////////////////
        t.addPut(3, put);
        System.out.println (nodeToString(t.root));
        f = t.visualize ();
        try { System.in.read (); } catch (Exception e) {}
        f.dispose ();
        ////////////////////////////////////////////////////
        t.addPut(1, put);
        System.out.println (nodeToString(t.root));
        f = t.visualize ();
        try { System.in.read (); } catch (Exception e) {}
        f.dispose ();
        ////////////////////////////////////////////////////
        t.shiftTime(1);
        System.out.println (nodeToString(t.root));
        f = t.visualize ();
        try { System.in.read (); } catch (Exception e) {}
        f.dispose ();
        ////////////////////////////////////////////////////
        t.addPut(4, put);
        System.out.println (nodeToString(t.root));
        f = t.visualize ();
        try { System.in.read (); } catch (Exception e) {}
        f.dispose ();
        ////////////////////////////////////////////////////
        t.shiftTime(2);
        System.out.println (nodeToString(t.root));
        f = t.visualize ();
        try { System.in.read (); } catch (Exception e) {}
        f.dispose ();
        ////////////////////////////////////////////////////
        t.addPut(4, put);
        System.out.println (nodeToString(t.root));
        f = t.visualize ();
        try { System.in.read (); } catch (Exception e) {}
        f.dispose ();
        ////////////////////////////////////////////////////
        System.out.println ("Exiting...");
        System.exit (0);
     }
}

