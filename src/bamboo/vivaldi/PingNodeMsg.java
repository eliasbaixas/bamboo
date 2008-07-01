/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.vivaldi;

import ostore.network.NetworkMessage;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;

/**
 * A message used to ping remote nodes for updating the local virtual
 * coordinate.  The sender node includes its current virtual coordinate
 * so that the remote node may use it update its own coordinate.
 * 
 * @author Steven Czerwinski
 * @version $Id: PingNodeMsg.java,v 1.1 2004/03/04 21:22:20 czerwin Exp $
 */

public class PingNodeMsg extends NetworkMessage {

  /**
   * The virtual coordinate for the node that was found.
   **/
  
  private VirtualCoordinate _sender_coord;
  
  /**
   * Constructs a ping message with the sender's virtual coordinate.
   *
   * @param dest The destination node.
   * @param coord The sender's current virtual coordinate.
   **/

  public PingNodeMsg(NodeId dest, VirtualCoordinate coord) {
    super (dest, false);
    _sender_coord = coord;
  }
  
  public PingNodeMsg(InputBuffer buffer) throws QSException { 
    super(buffer);
    _sender_coord = (VirtualCoordinate) buffer.nextObject();
  }

  public void serialize (OutputBuffer buffer) {
    super.serialize (buffer);
    buffer.add(_sender_coord);
  }

  public Object clone () throws CloneNotSupportedException {
    PingNodeMsg result = (PingNodeMsg) super.clone();
    result._sender_coord = _sender_coord;
    return result;
  }

  /**
   * Returns the virtual coordinate of the sender node.
   **/
  
  public VirtualCoordinate getSenderCoordinate() {
    return _sender_coord;
  }
  
  public String toString () {
    return new String("(PingNodeMsg peer=" + peer + " inbound=" + inbound +
                      " coordinate=" + _sender_coord + ")");
  }
}

