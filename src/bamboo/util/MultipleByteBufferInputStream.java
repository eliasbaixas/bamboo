/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.util;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;

/**
 * Wraps a java.nio.ByteBuffer in the ostore.util.InputBuffer interface.
 *
 * @author Sean C. Rhea
 * @version $Id: MultipleByteBufferInputStream.java,v 1.1 2005/01/24 03:30:42 srhea Exp $
 */
public class MultipleByteBufferInputStream extends InputStream {

    protected int size;
    protected LinkedList buffers = new LinkedList ();
    protected int remaining = -1;

    public void unlimit () { remaining = -1; }
    public void limit (int sz) { assert sz >= 0; remaining = sz; }
    public int limit_remaining () { assert remaining >= 0; return remaining; }

    public void add_bb (ByteBuffer bb) {
        if (bb.position () == bb.limit ())
            throw new IllegalArgumentException ();
        buffers.addLast (bb);
        size += bb.limit () - bb.position ();
    }

    public int read ()  { 
        if (buffers.isEmpty ()) 
            return -1;
        if (remaining == 0)
            return -1;
        ByteBuffer bb = (ByteBuffer) buffers.getFirst ();
        int result = ((int) bb.get ()) & 0xff;
        if (bb.position () == bb.limit ())
            buffers.removeFirst ();
        --size;
        if (remaining != -1)
            --remaining;
        return result;
    }

    public int read (byte [] output) {
        return read (output, 0, output.length);
    }

    public int read (byte [] output, int offset, int length) {
        return read_skip_impl (output, offset, length);
    }

    public int read_skip_impl (byte [] output, int offset, int length) {
        if (buffers.isEmpty ())
            return -1;
        if (remaining == 0)
            return -1;
        if (length > size) 
            length = size;
        size -= length;
        if (remaining != -1)
            if (length > remaining)
                length = remaining;
            remaining -= length;
        int bytes_read = 0;
        while (length > 0) {
            if (buffers.isEmpty ()) 
                throw new IllegalStateException ();
            ByteBuffer bb = (ByteBuffer) buffers.getFirst ();
            int toread = Math.min (length, bb.limit () - bb.position ());
            bytes_read += toread;
            if (output == null)
                bb.position (bb.position () + toread);
            else
                bb.get (output, offset, toread);
            offset += toread;
            length -= toread;
            if (bb.limit () == bb.position ())
                buffers.removeFirst ();
        }
        return bytes_read;
    }

    public long skip (long n) {
        assert n <= Integer.MAX_VALUE;
        return read_skip_impl (null, 0, (int) n);
    }
}

