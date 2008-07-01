/*
 * Copyright (c) 2005 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.openhash.fileshare;

import bamboo.dht.*;
import bamboo.lss.ASyncCore;
import bamboo.lss.DustDevil;
import bamboo.lss.PriorityQueue;
import bamboo.util.Pair;
import bamboo.util.StandardStage;
import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;
import org.apache.log4j.*;
import org.apache.commons.cli.*; 
import seda.sandStorm.api.ConfigDataIF;
import static bamboo.util.Curry.*;
import static bamboo.openhash.redir.RedirClient.bi2bytes;
import static bamboo.util.StringUtil.bytes_to_sbuf;
import static bamboo.util.StringUtil.*;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * A CFS-like file sharing program.
 *
 * @author Sean C. Rhea
 * @version $Id: FileShare.java,v 1.6 2005/04/27 20:51:11 srhea Exp $
 */
public class FileShare extends StandardStage {

    protected static final int BRANCHING = (1024 - 4) / 20;
    protected static final int MAX_BUFFER = 100;
    protected static final int MAX_PARALLEL = 100;
    protected static final String APPLICATION = "OpenDHT FileShare";

    protected int ttl = 0;
    protected MessageDigest md;
    protected GatewayClient client; 
    protected FileInputStream is; 
    protected FileOutputStream os; 
    protected byte [] secret; 
    protected LinkedList<Pair<byte[],ByteBuffer>> ready;
    protected Vector<LinkedList<ByteBuffer>> wblocks;
    protected Vector<PriorityQueue> rblocks;
    protected Vector<Long> rnext;
    protected int outstanding;
    protected byte [] key;
 
    public FileShare () throws Exception {
        md = MessageDigest.getInstance ("SHA");
    }

    public void init (ConfigDataIF config) throws Exception {
        super.init (config);
        final String mode = config_get_string (config, "mode");
        secret = config_get_string (config, "secret").getBytes ();
        String file = config_get_string (config, "file");
        String gwc = config_get_string (config, "client_stage_name");
        client = (GatewayClient) lookup_stage (config, gwc);
        if (mode.equals ("write")) {
            is = null;
            try {
                is = new FileInputStream (file);
            }
            catch (IOException e) {
                logger.error ("couldn't open file: " + file);
                System.exit (1);
            }
            ttl = config_get_int (config, "ttl");
            wblocks = new Vector<LinkedList<ByteBuffer>> (10);
            wblocks.add (new LinkedList<ByteBuffer> ());
            ready = new LinkedList<Pair<byte[],ByteBuffer>> ();
            for (int i = 0; i < MAX_PARALLEL; ++i) {
                acore.register_timer (0, 
                        new Runnable () { public void run() { write (); }});
            }
        }
        else if (mode.equals ("read")) {
            try {
                os = new FileOutputStream (file);
            }
            catch (IOException e) {
                logger.error ("couldn't open file: " + file);
                System.exit (1);
            }
            String kstr = config_get_string (config, "key");
            key = bi2bytes (new BigInteger (kstr.substring (2), 16));
            rblocks = new Vector<PriorityQueue> (10);
            rnext = new Vector<Long> (10);
            acore.register_timer (0, 
                    new Runnable () { public void run() { read (); }});
        }
        else {
            logger.error ("mode \"" + mode + "\" not supported");
            System.exit (1);
        }

        acore.register_timer (10*1000, new Runnable () { 
            public void run() {
                logger.info ("There are " + outstanding + 
                    (mode.equals ("read") ? " gets" : " puts") + 
                    " outstanding");
                acore.register_timer (10*1000, this);
            }
        });
    }

    /**
     * Transfer wblocks from the wblocks array to the ready queue.
     */
    public void make_parents (boolean done) {

        for (int l = 0; l < wblocks.size (); ++l) {
            logger.debug ("level " + l + " of " + wblocks.size () + " size=" + 
                          wblocks.elementAt (l).size () + " done=" + done);
            while ((wblocks.elementAt (l).size () >= BRANCHING) ||
                   (done && (wblocks.elementAt (l).size () > 1))) {
                int count = min (BRANCHING, wblocks.elementAt (l).size ());
                logger.debug ("count=" + count);
                for (int i = 0; i < count; ++i) {
                    ByteBuffer bb = wblocks.elementAt (l).removeFirst ();
                    bb.flip ();
                    md.update (secret);
                    md.update (bb.array (), 0, bb.limit ());
                    byte [] dig = md.digest ();
                    ready.addLast (new Pair<byte[],ByteBuffer> (dig,bb));
                    if (l+1 >= wblocks.size ()) {
                        wblocks.setSize (max (wblocks.size (), l + 2));
                        wblocks.setElementAt(new LinkedList<ByteBuffer>(), l+1);
                    }
                    LinkedList<ByteBuffer> next_level = wblocks.elementAt (l+1); 
                    if (next_level.isEmpty () ||
                        (next_level.getLast ().position () == 1024)) {
                        logger.debug ("adding a new block to level " + (l+1));
                        next_level.addLast (ByteBuffer.wrap (new byte [1024]));
                        next_level.getLast ().putInt (l+1);
                    }
                    logger.debug ("adding a digest to level " + (l+1));
                    next_level.getLast ().put (dig);
                }

                if (done) break;
            }
        }
        logger.debug ("make_parents done");
    }

    public void write () {
            logger.debug ("write");

            if (is != null) {
                while (ready.size () < MAX_BUFFER) {
                    ByteBuffer bb = ByteBuffer.wrap (new byte [1024]);
                    bb.putInt (0);
                    int len = 0;
                    try { len = is.read (bb.array (), 4, bb.limit () - 4); }
                    catch (IOException e) { is = null; break; }
                    if (len == -1) { is = null; break; }
                    logger.debug ("position=" + bb.position () + " read " +
                                  len + " bytes");
                    // We're going to flip this later, so set the position
                    // where we want the limit to end up.
                    bb.position (len+4); 
                    wblocks.elementAt (0).addLast (bb);
                    logger.debug ("read a block");
                    if (wblocks.elementAt (0).size () == BRANCHING) 
                        make_parents (false);
                }
                if (is == null) {
                    make_parents (true);
                    // There should now be only one non-empty level, at it
                    // should have exactly one block in it.
                    for (int l = 0; l < wblocks.size (); ++l) {
                        if (! wblocks.elementAt (l).isEmpty ()) {
                            ByteBuffer bb = wblocks.elementAt (l).removeFirst();
                            bb.flip ();
                            md.update (secret);
                            md.update (bb.array (), 0, bb.limit ());
                            byte [] dig = md.digest ();
                            StringBuffer sb = new StringBuffer (100);
                            bytes_to_sbuf (dig, 0, dig.length, false, sb);
                            logger.info ("root digest is 0x" + sb.toString ());
                            ready.addLast (new Pair<byte[],ByteBuffer>(dig,bb));
                            break;
                        }
                    }
                }
            }

            // Do put.

            if (ready.isEmpty ()) {
                if (outstanding == 0) {
                    logger.info ("all puts finished successfully");
                    System.exit (0);
                }
            }
            else {
                Pair<byte[],ByteBuffer> head = ready.removeFirst ();
                outstanding++;

                bamboo_put_args put = new bamboo_put_args ();
                put.application = APPLICATION;
                // GatewayClient will fill in put.client_library
                put.value = new bamboo_value ();
                if (head.second.limit () == head.second.array ().length)
                    put.value.value = head.second.array ();
                else {
                    put.value.value = new byte [head.second.limit ()];
                    head.second.get (put.value.value);
                }
                put.key = new bamboo_key ();
                put.key.value = head.first;
                put.ttl_sec = 3600; // TODO

                StringBuffer sb = new StringBuffer (100);
                bytes_to_sbuf (head.first, 0, head.first.length, false, sb);
                logger.debug ("putting block size=" + put.value.value.length
                        + " key=0x" + sb.toString ());
                client.put (put, curry(put_done_cb, put));
            }
    }

    public Thunk2<bamboo_put_args,Integer> put_done_cb = 
        new Thunk2<bamboo_put_args,Integer> () {
        public void run(final bamboo_put_args put, Integer result) {
            if (result.intValue () == 0) {
                outstanding--;
                write ();
            }
            else {
                StringBuffer sb = new StringBuffer (100);
                bytes_to_sbuf (put.key.value, 0, 
                        put.key.value.length, false, sb);
                final String key = sb.toString ();
                logger.debug ("got response " + result.intValue () + 
                        " for 0x" + key);
                acore.register_timer (1000, new Runnable () {
                        public void run() {
                            logger.debug ("reputting block 0x" + key);
                            client.put (put, curry(put_done_cb, put));
                        }
                    });
            }
        }
    };

    public void read () {
        if (rblocks.size () == 0) {
            rblocks.add (new PriorityQueue (BRANCHING));
            logger.debug ("setting level 0 need to 0");
            rnext.add (new Long (0));

            outstanding++;
            // Get the root block.
            bamboo_get_args get = new bamboo_get_args();
            get.application = APPLICATION;
            // GatewayClient will fill in get.client_library
            get.key = new bamboo_key();
            get.key.value = key;
            get.maxvals = 1;
            get.placemark = new bamboo_placemark();
            get.placemark.value = new byte[] {};

            logger.debug ("getting root: 0x" + bytes_to_str (key));
            client.get(get, curry(get_done_cb, get, new Long (0)));
        }
        else {
            boolean empty = true;

            while (outstanding < MAX_PARALLEL) {
            // Start from the bottom and work our way up.
            int l = 1; 
            for (; l < rblocks.size (); ++l) {
                if ((rblocks.elementAt (l) != null) && 
                    (! rblocks.elementAt (l).isEmpty ())) {
                    empty = false;

                    if (rblocks.elementAt (l).getFirstPriority () ==
                        rnext.elementAt (l).longValue ()) {

                        long p = rblocks.elementAt (l).getFirstPriority ();
                        ByteBuffer k = (ByteBuffer) 
                            rblocks.elementAt (l).removeFirst ();
                        logger.debug ("updating level " + l + 
                                      " need to " + (p+1));
                        rnext.setElementAt (new Long (p+1), l);
                        logger.debug ("level " + l + 
                                      " has need " + rnext.elementAt (l));

                        outstanding++;
                        // Get this block.
                        bamboo_get_args get = new bamboo_get_args();
                        get.application = APPLICATION;
                        // GatewayClient will fill in get.client_library
                        get.key = new bamboo_key();
                        get.key.value = k.array ();
                        get.maxvals = 1;
                        get.placemark = new bamboo_placemark();
                        get.placemark.value = new byte[] {};

                        logger.debug ("getting lev=" + l + " pos=" + p +
                                " key=0x" + bytes_to_str (k.array ()));

                        client.get(get, curry(get_done_cb, get, new Long (p)));
                        break;
                    }
                    else {
                        logger.debug ("level " + l + " have " +
                                rblocks.elementAt (l).getFirstPriority () +
                                " need " + rnext.elementAt (l).longValue ());
                    }
                }
            }
            if (l == rblocks.size ())
                break;
            }

            if (outstanding == 0) {
                assert empty;
                logger.info ("all gets finished successfully");
                try { os.close (); } catch (IOException e) {
                    logger.error ("could not close output file");
                    System.exit (1);
                }
                System.exit (0);
            }
        }
    }

    public Thunk3<bamboo_get_args,Long,bamboo_get_res> get_done_cb = 
        new Thunk3<bamboo_get_args,Long,bamboo_get_res> () {
        public void run(final bamboo_get_args get, final Long pos, 
                        bamboo_get_res result) {
            if (result.values.length == 0) {
                StringBuffer sb = new StringBuffer (100);
                bytes_to_sbuf (get.key.value, 0, 
                        get.key.value.length, false, sb);
                final String key = sb.toString ();
                logger.debug ("got empty response for 0x" + key);
                acore.register_timer (1000, new Runnable () {
                        public void run() {
                            logger.debug ("trying to get 0x" + key + " again");
                            client.get (get, curry(get_done_cb, get, pos));
                        }
                    });
            }
            else {
                assert result.values.length == 1;
                logger.debug ("got 0x" + bytes_to_str(get.key.value));
                outstanding--;
                ByteBuffer bb = ByteBuffer.wrap (result.values [0].value);
                int l = bb.getInt ();
                if ((rblocks.size () < l + 1) || 
                    (rblocks.elementAt (l) == null)) {
                    rblocks.setSize (max (rblocks.size (), l + 1));
                    rblocks.setElementAt (new PriorityQueue (BRANCHING), l);
                    logger.debug ("setting level " + l + " need to 0");
                    rnext.setSize (max (rnext.size (), l + 1));
                    rnext.setElementAt (new Long (0), l);
                }
                if (l == 0) {
                    rblocks.elementAt (l).add (bb, pos.longValue ());
                }
                else {
                    long p = BRANCHING * pos.longValue ();
                    while (bb.position () < bb.limit ()) {
                        byte [] k = new byte [20];
                        bb.get (k);
                        logger.debug ("need to get lev=" + l + " pos=" + p + 
                                " key=0x" + bytes_to_str (k));
                        rblocks.elementAt (l).add (ByteBuffer.wrap (k), p++);
                    }
                }
            }

            // Write any blocks we can to disk, then call read again.
            while ((! rblocks.elementAt (0).isEmpty ()) && 
                   (rnext.elementAt (0).longValue () ==
                    rblocks.elementAt (0).getFirstPriority ())) {
                long p = rblocks.elementAt (0).getFirstPriority ();
                ByteBuffer bb = (ByteBuffer) 
                    rblocks.elementAt (0).removeFirst ();
                logger.debug ("wrote block; updating level 0 need to " + (p+1));
                rnext.setElementAt (new Long (p + 1), 0);
                try {
                    logger.debug ("position=" + bb.position () + " writing " +
                                  (bb.limit () - bb.position ()) + " bytes");
                    os.write (bb.array (), bb.arrayOffset () + bb.position (), 
                              bb.limit () - bb.position ());
                }
                catch (IOException e) {
                    logger.error ("could not write to output file");
                    System.exit (1);
                }
            }

            read ();
        }
    };

    public static void main (String [] args) throws Exception {
        PatternLayout pl = new PatternLayout ("%d{ISO8601} %-5p %c: %m\n");
        ConsoleAppender ca = new ConsoleAppender (pl);
        Logger.getRoot ().addAppender (ca);
        Logger.getRoot ().setLevel (Level.INFO);

        // create Options object
        Options options = new Options();

        // add t option
        options.addOption ("r", "read",  false, "read a file from the DHT");
        options.addOption ("w", "write", false, "write a file to the DHT");
        options.addOption ("g", "gateway", true, "the gateway IP:port");
        options.addOption ("k", "key", true, "the key to read a file from");
        options.addOption ("f", "file", true, "the file to read or write");
        options.addOption ("s", "secret", true, "the secret used to hide data");
        options.addOption ("t", "ttl", true, 
                          "how long in seconds data should persist");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse( options, args);

        String gw = null;
        String mode = null;
        String secret = null;
        String ttl = null;
        String key = null;
        String file = null;

        if (cmd.hasOption ("r")) { mode = "read"; }
        if (cmd.hasOption ("w")) { mode = "write"; }
        if (cmd.hasOption ("g")) { gw = cmd.getOptionValue("g"); }
        if (cmd.hasOption ("k")) { key = cmd.getOptionValue("k"); }
        if (cmd.hasOption ("f")) { file = cmd.getOptionValue("f"); }
        if (cmd.hasOption ("s")) { secret = cmd.getOptionValue("s"); }
        if (cmd.hasOption ("t")) { ttl = cmd.getOptionValue("t"); }

        if (mode == null) {
            System.err.println ("ERROR: either --read or --write is required");
            HelpFormatter formatter = new HelpFormatter ();
            formatter.printHelp ("fileshare", options);
            System.exit (1);
        }

        if (gw == null) {
            System.err.println ("ERROR: --gateway is required");
            HelpFormatter formatter = new HelpFormatter ();
            formatter.printHelp ("fileshare", options);
            System.exit (1);
        }

        if (file == null) {
            System.err.println ("ERROR: --file is required");
            HelpFormatter formatter = new HelpFormatter ();
            formatter.printHelp ("fileshare", options);
            System.exit (1);
        }

        if (secret == null) {
            System.err.println ("ERROR: --secret is required");
            HelpFormatter formatter = new HelpFormatter ();
            formatter.printHelp ("fileshare", options);
            System.exit (1);
        }

        StringBuffer sbuf = new StringBuffer (1000);
        sbuf.append ("<sandstorm>\n");
        sbuf.append ("<global>\n");
        sbuf.append ("<initargs>\n");
        sbuf.append ("node_id localhost:3630\n");
        sbuf.append ("</initargs>\n");
        sbuf.append ("</global>\n");
        sbuf.append ("<stages>\n");
        sbuf.append ("<GatewayClient>\n");
        sbuf.append ("class bamboo.dht.GatewayClient\n");
        sbuf.append ("<initargs>\n");
        sbuf.append ("debug_level 0\n");
        sbuf.append ("gateway " + gw + "\n");
        sbuf.append ("</initargs>\n");
        sbuf.append ("</GatewayClient>\n");
        sbuf.append ("\n");
        sbuf.append ("<FileShare>\n");
        sbuf.append ("class bamboo.openhash.fileshare.FileShare\n");
        sbuf.append ("<initargs>\n");
        sbuf.append ("debug_level 0\n");
        sbuf.append ("secret " + secret + "\n");
        sbuf.append ("mode " + mode + "\n");
        if (mode.equals ("write")) {
            if (ttl == null) {
                System.err.println ("ERROR: --ttl is required for write mode");
                HelpFormatter formatter = new HelpFormatter ();
                formatter.printHelp ("fileshare", options);
                System.exit (1);
            }

            sbuf.append ("ttl " + ttl + "\n"); 
            sbuf.append ("file " + file + "\n"); 
        }
        else {
            if (key == null) {
                System.err.println ("ERROR: --key is required for write mode");
                HelpFormatter formatter = new HelpFormatter ();
                formatter.printHelp ("fileshare", options);
                System.exit (1);
            }

            sbuf.append ("key " + key + "\n"); 
            sbuf.append ("file " + file + "\n"); 
        }
        sbuf.append ("client_stage_name GatewayClient\n");
        sbuf.append ("</initargs>\n");
        sbuf.append ("</FileShare>\n");
        sbuf.append ("</stages>\n");
        sbuf.append ("</sandstorm>\n");
        ASyncCore acore = new bamboo.lss.ASyncCoreImpl();
        DustDevil dd = new DustDevil ();
        dd.set_acore_instance (acore);
        dd.main (new CharArrayReader (sbuf.toString ().toCharArray ()));
        acore.async_main ();
    }
}

