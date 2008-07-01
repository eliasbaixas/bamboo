/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.util;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import ostore.util.InputBuffer;
import ostore.util.InputBufferImpl;
import ostore.util.OutputBuffer;

/**
 * Wraps a java.io.InputStream in the ostore.util.InputBuffer interface.
 *
 * @author Sean C. Rhea
 * @version $Id: StreamInputBuffer.java,v 1.1 2004/05/22 00:15:44 srhea Exp $
 */
public class StreamInputBuffer extends ostore.util.InputBufferImpl {

    public InputStream is;
    public LinkedList errors;

    public StreamInputBuffer (InputStream i) { is = i; }

    public byte nextByte () {
        try { return (byte) is.read (); }
        catch (IOException e) { error (e); return (byte) 0; }
    }

    public void nextBytes (byte [] output, int offset, int length) {
	try { is.read (output, offset, length); }
        catch (IOException e) { error (e); }
    }

    public void nextBytes (OutputBuffer buffer) {
        throw new NoSuchMethodError ();
    }

    public void skipBytes (int length) {
        try { is.skip (length); }
        catch (IOException e) { error (e); }
    }

    public InputBuffer subBuffer (int length) {
        throw new NoSuchMethodError ();
    }

    protected void error (IOException e) {
        if (errors == null)
            errors = new LinkedList ();
        errors.addLast (e);
    }
}
