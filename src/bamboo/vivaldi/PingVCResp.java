/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.vivaldi;

import bamboo.router.PingMsg;
import ostore.network.NetworkMessage;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;

/**
 * A ping response containing the node's virtual coordinate.  
 *
 * @author  Steven E. Czerwinski
 * @version $Id: PingVCResp.java,v 1.2 2004/03/09 22:04:37 czerwin Exp $
 */
public class PingVCResp extends NetworkMessage {

    public VirtualCoordinate coordinate;
    public int version_number;
    public PingVCResp (PingMsg msg, VirtualCoordinate coord, int version_number) {
        super (msg.peer, false);
        coordinate = coord;
        this.version_number = version_number;
    }

    public PingVCResp (InputBuffer buffer) throws QSException {
        super (buffer);
        version_number = buffer.nextInt();
        coordinate = (VirtualCoordinate) buffer.nextObject();
    }

    public void serialize (OutputBuffer buffer) {
        super.serialize (buffer);
        buffer.add(version_number);
        buffer.add(coordinate);
    }

    public Object clone () throws CloneNotSupportedException {
        PingVCResp result = (PingVCResp) super.clone ();
        result.coordinate = coordinate;
        result.version_number = version_number;
        return result;
    }

    public String toString () {
      return "(PingResp " + super.toString() + " vc=" + coordinate + " vn="
        + version_number+")";
    }
}

