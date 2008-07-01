/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.lss;
import java.nio.ByteBuffer;
import ostore.util.ByteArrayInputBuffer;
import ostore.util.ByteArrayOutputBuffer;
import ostore.util.InputBuffer;
import ostore.util.InputBufferImpl;
import ostore.util.OutputBuffer;

/**
 * Wraps a java.nio.ByteBuffer in the ostore.util.InputBuffer interface.
 *
 * @author Sean C. Rhea
 * @version $Id: NioInputBuffer.java,v 1.6 2003/10/05 18:22:11 srhea Exp $
 */
public class NioInputBuffer extends ostore.util.InputBufferImpl {

    public ByteBuffer bb;

    public NioInputBuffer (ByteBuffer bb) { this.bb = bb; }

    public int   nextInt ()   { return bb.getInt (); }
    public short nextShort () { return bb.getShort (); }
    public long  nextLong ()  { return bb.getLong (); }
    public byte  nextByte ()  { return bb.get (); }

    public void nextBytes (byte [] output, int offset, int length) {
	bb.get (output, offset, length);
    }

    public void nextBytes (OutputBuffer buffer) {
	if (buffer instanceof NioOutputBuffer) {
	    ((NioOutputBuffer) buffer).bb.put (bb);
	}
	else if (buffer instanceof ByteArrayOutputBuffer) {
	    ByteArrayOutputBuffer ob = (ByteArrayOutputBuffer) buffer;
	    bb.get (ob.data (), ob.offset (), bb.remaining ());
	}
	else {
	    while (bb.hasRemaining ())
		buffer.add (bb.get ());
	}
    }

    public void skipBytes (int length) {
	if (bb.position () + length > bb.limit ())
	    throw new IndexOutOfBoundsException ();
	bb.position (bb.position () + length);
    }

    public InputBuffer subBuffer (int length) {
	byte [] data = new byte [length];
	bb.get (data, 0, length);
	return new ByteArrayInputBuffer (data);
    }
}

