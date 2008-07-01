/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */
package bamboo.util;
import org.acplt.oncrpc.XdrAble;
import java.nio.ByteBuffer;
import bamboo.dht.bamboo_value;
import org.acplt.oncrpc.XdrDecodingStream;

/**
 * Clone XdrAble objects by serializing and deserializing them.
 *
 * @author  Sean C. Rhea
 * @version $Id: XdrClone.java,v 1.2 2004/04/19 22:56:39 srhea Exp $
 */
public class XdrClone {

    /**
     * Clone XdrAble objects by serializing and deserializing them; the buffer
     * is cleared before serialization and not cleared after deserialization,
     * so the size of the object can be found by calling buf.position () after
     * a call to xdr_clone.
     */
    public static XdrAble xdr_clone (XdrAble value, ByteBuffer buf) {
        buf.clear();
        XdrByteBufferEncodingStream es = new XdrByteBufferEncodingStream(buf);
        try {
            value.xdrEncode(es);
            buf.flip();
            XdrByteBufferDecodingStream ds =
                new XdrByteBufferDecodingStream(buf);
            ds.beginDecoding();
            return (XdrAble) value.getClass().
                getConstructor(new Class[] {XdrDecodingStream.class}).
                newInstance(new Object[] {ds});
        }
        catch (Exception e) {
            assert false:e;
            return null; // in case asserts are turned off
        }
    }

    /**
     * Test harness.
     */
    public static void main (String [] args) {
        ByteBuffer b = ByteBuffer.allocate (100);
        bamboo_value v = new bamboo_value ();
        v.value = new byte [10];
        bamboo_value u = (bamboo_value) xdr_clone (v, b);
        System.err.println ("size=" + b.position ());
    }
}
