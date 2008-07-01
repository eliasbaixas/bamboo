/*
 * $Header: /home/srhea/cvsroot/bamboo/src/bamboo/util/XdrByteBufferEncodingStream.java,v 1.1 2004/02/06 03:55:49 srhea Exp $
 *
 * Copyright (c) 1999, 2000
 * Lehrstuhl fuer Prozessleittechnik (PLT), RWTH Aachen
 * D-52064 Aachen, Germany.
 * All rights reserved.
 *
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */

package bamboo.util;
import org.acplt.oncrpc.*;
import java.io.*;
import java.net.*;
import java.nio.*;

/**
 * The <code>XdrBufferEncodingStream</code> class provides a ByteBuffer-based
 * XDR stream.  Cribbed by Sean Rhea from the class
 * org.acplt.oncrpc.XdrBufferEncodingStream.
 *
 * @version $Revision: 1.1 $ $Date: 2004/02/06 03:55:49 $ $State: Exp $ $Locker:  $
 * @author Harald Albrecht
 * @author Sean Rhea
 */
public class XdrByteBufferEncodingStream extends XdrEncodingStream {

    /**
     * Constructs a new <code>XdrBufferEncodingStream</code> with a buffer
     * to encode data into of the given size.
     *
     * @param bufferSize Size of buffer to store encoded data in.
     */
    public XdrByteBufferEncodingStream(int bufferSize) {
        if ( (bufferSize < 0)
             || (bufferSize & 3) != 0 ) {
            throw(new IllegalArgumentException("size of buffer must be a multiple of four and must not be negative"));
        }
        this.buffer = ByteBuffer.allocate (bufferSize);
        bufferIndex = 0;
        bufferHighmark = bufferSize - 4;
    }

    /**
     * Constructs a new <code>XdrBufferEncodingStream</code> with a given
     * buffer.
     *
     * @param buffer Buffer to store encoded information in.
     *
     * @throws IllegalArgumentException if <code>encodedLength</code> is not
     *   a multiple of four.
     */
    public XdrByteBufferEncodingStream(ByteBuffer buffer) {
        //
        // Make sure that the buffer size is a multiple of four, otherwise
        // throw an exception.
        //
        if ( ((buffer.limit () - buffer.position ()) & 3) != 0 ) {
            throw(new IllegalArgumentException("size of buffer must be a multiple of four"));
        }
        this.buffer = buffer;
        bufferIndex = 0;
        bufferHighmark = buffer.limit () - buffer.position () - 4;
    }

    /**
     * Returns the amount of encoded data in the buffer.
     *
     * @return length of data encoded in buffer.
     */
    public int getXdrLength() {
        return bufferIndex;
    }

    /**
     * Returns the buffer holding encoded data.
     *
     * @return Buffer with encoded data.
     */
    public ByteBuffer getXdrData() {
        return buffer;
    }

    /**
     * Begins encoding a new XDR record. This involves resetting this
     * encoding XDR stream back into a known state.
     *
     * @param receiver Indicates the receiver of the XDR data. This can be
     *   <code>null</code> for XDR streams connected permanently to a
     *   receiver (like in case of TCP/IP based XDR streams).
     * @param receiverPort Port number of the receiver.
     *
     * @throws OncRpcException if an ONC/RPC error occurs.
     * @throws IOException if an I/O error occurs.
     */
    public void beginEncoding(InetAddress receiverAddress, int receiverPort)
           throws OncRpcException, IOException {
        bufferIndex = 0;
    }

    /**
     * Flushes this encoding XDR stream and forces any buffered output bytes
     * to be written out. The general contract of <code>endEncoding</code> is that
     * calling it is an indication that the current record is finished and any
     * bytes previously encoded should immediately be written to their intended
     * destination.
     *
     * @throws OncRpcException if an ONC/RPC error occurs.
     * @throws IOException if an I/O error occurs.
     */
    public void endEncoding()
           throws OncRpcException, IOException {
    }

    /**
     * Closes this encoding XDR stream and releases any system resources
     * associated with this stream. The general contract of <code>close</code>
     * is that it closes the encoding XDR stream. A closed XDR stream cannot
     * perform encoding operations and cannot be reopened.
     *
     * @throws OncRpcException if an ONC/RPC error occurs.
     * @throws IOException if an I/O error occurs.
     */
    public void close()
           throws OncRpcException, IOException {
        buffer = null;
    }

    /**
     * Encodes (aka "serializes") a "XDR int" value and writes it down a
     * XDR stream. A XDR int is 32 bits wide -- the same width Java's "int"
     * data type has. This method is one of the basic methods all other
     * methods can rely on.
     *
     * @throws OncRpcException if an ONC/RPC error occurs.
     * @throws IOException if an I/O error occurs.
     */
    public void xdrEncodeInt(int value)
           throws OncRpcException, IOException {
        if ( bufferIndex <= bufferHighmark ) {
            //
            // There's enough space in the buffer, so encode this int as
            // four bytes (french octets) in big endian order (that is, the
            // most significant byte comes first.
            //
            buffer.putInt (value);
            bufferIndex += 4;
        } else {
            throw(new OncRpcException(OncRpcException.RPC_BUFFEROVERFLOW));
        }
    }

    /**
     * Encodes (aka "serializes") a XDR opaque value, which is represented
     * by a vector of byte values, and starts at <code>offset</code> with a
     * length of <code>length</code>. Only the opaque value is encoded, but
     * no length indication is preceeding the opaque value, so the receiver
     * has to know how long the opaque value will be. The encoded data is
     * always padded to be a multiple of four. If the given length is not a
     * multiple of four, zero bytes will be used for padding.
     *
     * @throws OncRpcException if an ONC/RPC error occurs.
     * @throws IOException if an I/O error occurs.
     */
    public void xdrEncodeOpaque(byte [] value, int offset, int length)
           throws OncRpcException, IOException {
        //
        // First calculate the number of bytes needed for padding.
        //
        int padding = (4 - (length & 3)) & 3;
        if ( bufferIndex <= bufferHighmark - (length + padding) ) {
            buffer.put (value, offset, length);
            bufferIndex += length;
            if ( padding != 0 ) {
                //
                // If the length of the opaque data was not a multiple, then
                // pad with zeros, so the write pointer (argh! how comes Java
                // has a pointer...?!) points to a byte, which has an index
                // of a multiple of four.
                //
                buffer.put(paddingZeros, 0, padding);
                bufferIndex += padding;
            }
        } else {
            throw(new OncRpcException(OncRpcException.RPC_BUFFEROVERFLOW));
        }
    }

    /**
     * The buffer which will receive the encoded information, before it
     * is sent via a datagram socket.
     */
    private ByteBuffer buffer;

    /**
     * The write pointer is an index into the <code>buffer</code>.
     */
    private int bufferIndex;

    /**
     * Index of the last four byte word in the buffer.
     */
    private int bufferHighmark;

    /**
     * Some zeros, only needed for padding -- like in real life.
     */
    private static final byte [] paddingZeros = { 0, 0, 0, 0 };

}

// End of XdrBufferEncodingStream.java
