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
 * A response for finding a random node in the bamboo overlay.  This
 * message is sent directly from the located node to the sender of the
 * request.  The located node includes its nodeid and current virtual
 * coordinate.
 * 
 * @author Steven Czerwinski
 * @version $Id: LocateNodeResp.java,v 1.1 2004/03/04 21:22:20 czerwin Exp $
 */

public class LocateNodeResp extends NetworkMessage {

  /**
   * The virtual coordinate for the node that was found.
   **/
  
  private VirtualCoordinate _located_coord;

  /**
   * The network address of the node that was found.
   **/
  private NodeId _located_id;
  
  /**
   * Constructs a response from the located node to be sent to the requestor.
   *
   * @param requestor The node which sent the original message.
   * @param locatedId The node that was discovered.
   * @param locatedCoordinate The discovered node's virtual coordinate.
   **/

  public LocateNodeResp(NodeId requestor, NodeId locatedId,
                        VirtualCoordinate locatedCoordinate) {
    super (requestor, false);
    _located_coord = locatedCoordinate;
    _located_id = locatedId;
  }
  
  public LocateNodeResp(InputBuffer buffer) throws QSException { 
    super(buffer);
    _located_coord = (VirtualCoordinate) buffer.nextObject();
    _located_id = (NodeId) buffer.nextObject();
  }

  public void serialize (OutputBuffer buffer) {
    super.serialize (buffer);
    buffer.add(_located_coord);
    buffer.add(_located_id);
  }

  public Object clone () throws CloneNotSupportedException {
    LocateNodeResp result = (LocateNodeResp) super.clone();
    result._located_coord = _located_coord;
    result._located_id = _located_id;
    return result;
  }

  /**
   * Returns the node id of the discovered node.
   **/
  
  public NodeId getLocatedId() {
    return _located_id;
  }

  /**
   * Returns the virtual coordinate of the discovered noded.
   **/
  
  public VirtualCoordinate getLocatedCoordinate() {
    return _located_coord;
  }
  
  public String toString () {
    return new String("(LocateNodeResp peer=" + peer + " inbound=" + inbound +
                      " coordinate=" + _located_coord + ")");
  }
}

