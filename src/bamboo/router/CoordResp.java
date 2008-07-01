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
 * Coordinate response type.
 *
 * @author Sean C. Rhea
 * @version $Id: CoordResp.java,v 1.1 2005/08/15 21:49:06 srhea Exp $
 */
public class CoordResp implements QuickSerializable {
    public VirtualCoordinate coords;
    public CoordResp(VirtualCoordinate c) { coords = c; }
    public CoordResp(InputBuffer buffer) throws QSException {
        coords = new VirtualCoordinate(buffer);
    }
    public void serialize(OutputBuffer buffer) { coords.serialize(buffer); }
}

