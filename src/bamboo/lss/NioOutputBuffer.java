/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.lss;
import java.nio.ByteBuffer;
import ostore.util.OutputBuffer;
import ostore.util.OutputBufferImpl;

/**
 * Wraps a java.nio.ByteBuffer in the ostore.util.OutputBuffer interface.
 *
 * @author Sean C. Rhea
 * @version $Id: NioOutputBuffer.java,v 1.6 2003/10/05 18:22:11 srhea Exp $
 */
public class NioOutputBuffer extends OutputBufferImpl {

    public ByteBuffer bb;

    public NioOutputBuffer (ByteBuffer bb) {
	this.bb = bb;
    }

    public int offset () { return bb.position (); } 
    public int size () { return bb.position (); }

    public void add (byte[] value, int offset, int length) {
	bb.put (value, offset, length);
    }

    public void add (byte value)  { bb.put (value); } 
    public void add (int value)   { bb.putInt (value); } 
    public void add (short value) { bb.putShort (value); } 
    public void add (long value)  { bb.putLong (value); } 
}

