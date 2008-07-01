/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.util;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import ostore.util.OutputBufferImpl;

/**
 * Wraps a java.io.OutputStream in the ostore.util.OutputBuffer interface.
 *
 * @author Sean C. Rhea
 * @version $Id: StreamOutputBuffer.java,v 1.1 2004/05/22 00:15:44 srhea Exp $
 */
public class StreamOutputBuffer extends OutputBufferImpl {

    public OutputStream os;
    public LinkedList errors;

    public StreamOutputBuffer (OutputStream o) { os = o; }

    public void add (byte[] value, int offset, int length) {
        try { os.write (value, offset, length); }
        catch (IOException e) { error (e); }
    }

    public void add (byte value)  {
        try { os.write (value); }
        catch (IOException e) { error (e); }
    }

    protected void error (IOException e) {
        if (errors == null)
            errors = new LinkedList ();
        errors.addLast (e);
    }
}

