/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.lss;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import ostore.util.ByteArrayInputBuffer;
import ostore.util.ByteArrayOutputBuffer;
import ostore.util.InputBuffer;
import ostore.util.InputBufferImpl;
import ostore.util.OutputBuffer;

/**
 * Wraps a java.nio.ByteBuffer in the ostore.util.InputBuffer interface.
 *
 * @author Sean C. Rhea
 * @version $Id: NioMultiplePacketInputBuffer.java,v 1.2 2004/11/14 00:48:46 srhea Exp $
 */
public class NioMultiplePacketInputBuffer extends ostore.util.InputBufferImpl {

    protected int size;
    protected LinkedList buffers = new LinkedList ();
    protected int remaining = -1;

    public void unlimit () { remaining = -1; }
    public void limit (int sz) { assert sz >= 0; remaining = sz; }
    public int limit_remaining () { assert remaining >= 0; return remaining; }

    public int size () {
        return size;
    }

    public void add_packet (ByteBuffer bb) {
        if (bb.position () == bb.limit ())
            throw new IllegalArgumentException ();
        buffers.addLast (bb);
        size += bb.limit () - bb.position ();
    }

    public byte nextByte ()  { 
        if (buffers.isEmpty ()) 
            throw new IllegalStateException ();
        if (remaining == 0)
            throw new IllegalStateException ();
        ByteBuffer bb = (ByteBuffer) buffers.getFirst ();
        byte result = bb.get ();
        if (bb.position () == bb.limit ())
            buffers.removeFirst ();
        --size;
        if (remaining != -1)
            --remaining;
        return result;
    }

    public void nextBytes (byte [] output, int offset, int length) {
        size -= length;
        if (remaining != -1) {
            if (length > remaining)
                throw new IllegalStateException ();
            remaining -= length;
        }
        while (length > 0) {
            if (buffers.isEmpty ()) 
                throw new IllegalStateException ();
            ByteBuffer bb = (ByteBuffer) buffers.getFirst ();
            int toread = Math.min (length, bb.limit () - bb.position ());
            bb.get (output, offset, toread);
            offset += toread;
            length -= toread;
            if (bb.limit () == bb.position ())
                buffers.removeFirst ();
        }
    }

    public void nextBytes (OutputBuffer buffer) {
        throw new NoSuchMethodError ();
    }

    public InputBuffer subBuffer (int length) {
        throw new NoSuchMethodError ();
    }
}

