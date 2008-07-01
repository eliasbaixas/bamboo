/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dmgr;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import bamboo.db.StorageManager;
import bamboo.util.GuidTools;

/**
 * A test harness for bamboo.dmgr.MerkleTree.
 *
 * @author Sean C. Rhea
 * @version $Id: MerkleTreeTest.java,v 1.10 2005/05/12 00:08:19 srhea Exp $
 */
public class MerkleTreeTest {

    public static void draw_tree (
            MerkleTree.Node root, SortedSet keys, String prefix) {

        System.out.println (prefix + root);
        if (root.has_children ()) {
            for (MerkleTree.Node.Iter i = root.children (); i.hasNext (); ) {
                MerkleTree.Node child = i.next ();
                if (child != null)
                    draw_tree (child, keys, prefix + "  ");
            }
        }
        else {
            StorageManager.Key low = new StorageManager.Key (
                    root.range_low (), 0, min_guid, min_hash, min_hash, false,
                    StorageManager.ZERO_CLIENT);
            StorageManager.Key high = new StorageManager.Key (
                    root.range_high (), 0, max_guid, max_hash, max_hash, true,
                    StorageManager.MAX_CLIENT);

            if (keys != null) {
                SortedSet under_me = keys.headSet (high).tailSet (low);

                for (Iterator i = under_me.iterator (); i.hasNext (); ) {
                    StorageManager.Key k = (StorageManager.Key) i.next ();
                    System.out.println (prefix + "  "
                            + Long.toHexString (k.time_usec));
                }
            }
        }
    }

    public static void fill_holes (MerkleTree tree, SortedSet keys) {

        MerkleTree.FillHolesState state = null;
        while (true) {

            state = tree.root ().fill_holes (
                    state, System.currentTimeMillis ());
            if (state == null)
                break;

            StorageManager.Key low = new StorageManager.Key (
                    state.range_low, 0, min_guid, min_hash, min_hash, false,
                    StorageManager.ZERO_CLIENT);
            StorageManager.Key high = new StorageManager.Key (
                    state.range_high, 0, max_guid, max_hash, max_hash, true,
                    StorageManager.MAX_CLIENT);

            SortedSet under_me = keys.headSet (high).tailSet (low);

            state.leaves_below = under_me.size ();
            if (state.leaves_below <= state.max_leaves_below) {
                for (Iterator i = under_me.iterator (); i.hasNext (); ) {
                    StorageManager.Key k = (StorageManager.Key) i.next ();
                    byte [] bytes = new byte [StorageManager.Key.SIZE];
                    k.to_byte_buffer (ByteBuffer.wrap (bytes, 0, bytes.length));
                    md.update (bytes);
                }
                state.digest = md.digest ();
            }
            else {
                state.digest = null;
            }
        }
    }

    public static BigInteger min_guid = BigInteger.valueOf (0);
    public static BigInteger max_guid;
    public static byte [] min_hash;
    public static byte [] max_hash;
    public static MessageDigest md;

    public static void main (String [] args) throws Exception {

        if (args.length < 4) {
            System.err.println (
                    "usage: <seed> <expansion> <to add> <to remove>");
            System.exit (1);
        }

        md = MessageDigest.getInstance ("SHA");
        // Find out the size of the digest.
        md.update ((byte) 0);
        max_hash = md.digest ();
        min_hash = new byte [max_hash.length];
        for (int i = 0; i < max_hash.length; ++i)
            max_hash [i] = -1;

        int seed = Integer.parseInt (args [0]);
        Random rand = new Random (seed);

        int expansion = Integer.parseInt (args [1]);

        max_guid = BigInteger.valueOf (2).pow (160).subtract (BigInteger.ONE);

        MerkleTree tree = new MerkleTree (expansion, md);
        SortedSet keys = new TreeSet ();

        System.out.println ("Starting tree:");
        draw_tree (tree.root (), keys, "  ");

        int to_add = Integer.parseInt (args [2]);
        for (int i = 0; i < to_add; ++i) {
            byte [] data_hash = new byte [20];
            rand.nextBytes (data_hash);
            long time_usec = rand.nextLong () & 0x7fffffffffffffffL;
            // TODO: fudge
            while (time_usec >= tree.root ().range_high ())
                time_usec >>= 1;
            StorageManager.Key k = new StorageManager.Key (
                    time_usec, 0, GuidTools.random_guid (rand),
                    new byte[20], data_hash, rand.nextInt (2) > 0,
                    StorageManager.ZERO_CLIENT);
            System.out.println ("adding key[" + i + "]=" + k);

            keys.add (k);
            tree.root ().invalidate_path (k.time_usec);
            fill_holes (tree, keys);

            System.out.println ("After add " + k);
            draw_tree (tree.root (), keys, "  ");
        }

        int to_remove = Integer.parseInt (args [3]);
        for (int i = 0; i < to_remove; ++i) {
            int which = rand.nextInt (keys.size ());
            StorageManager.Key k = null;
            Iterator j = keys.iterator ();
            while (which-- >= 0)
                k = (StorageManager.Key) j.next ();

            System.out.println ("removing key[" + i + "]=" + k);

            j.remove ();
            tree.root ().invalidate_path (k.time_usec);
            fill_holes (tree, keys);

            System.out.println ("After remove " + k);
            draw_tree (tree.root (), keys, "  ");
        }

        System.out.println ("Final tree:");
        draw_tree (tree.root (), keys, "  ");
    }

}

