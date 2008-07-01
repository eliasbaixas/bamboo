/*
 * Copyright (c) 2005 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.router;
import bamboo.vivaldi.VirtualCoordinate;
import java.math.BigInteger;
import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;

/**
 * Coordinate request type.
 *
 * @author Sean C. Rhea
 * @version $Id: CoordReq.java,v 1.1 2005/08/15 21:49:06 srhea Exp $
 */
public class CoordReq implements QuickSerializable {
    public BigInteger srcID;
    public VirtualCoordinate srcCoords;
    public CoordReq(BigInteger i, VirtualCoordinate c) {
        srcID = i; srcCoords = c;
    }
    public CoordReq(InputBuffer buffer) throws QSException {
        srcID = buffer.nextBigInteger();
        srcCoords = new VirtualCoordinate(buffer);
    }
    public void serialize(OutputBuffer buffer) {
        buffer.add(srcID);
        srcCoords.serialize(buffer);
    }
}

