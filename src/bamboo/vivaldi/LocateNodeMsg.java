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
import ostore.util.QuickSerializable;
import seda.sandStorm.api.QueueElementIF;

/**
 * A message used to pick a random node in the bamboo overlay for updating
 * the sender's virtual coordinate.  Upon receipt, a node will respond with
 * a <tt>LocateNodeResp</tt> to tell the sender it's node id and current
 * virtual coordinate.
 *
 * <p>
 * The sending node also includes its current virtual coordinate, in case
 * the remote node wishes to use the response message to update its own
 * virtual coordinate.
 *
 * @author  Steven Czerwinski
 * @version $Id: LocateNodeMsg.java,v 1.2 2004/03/09 22:04:37 czerwin Exp $ */

public class LocateNodeMsg implements QuickSerializable, QueueElementIF {

  /**
   * The orginiator of this request.
   **/
  private NodeId _requestor;

  /**
   * The orginator's virtual coordinate.
   **/
  
  private VirtualCoordinate _coordinate;

  /**
   * The requesting node's virtual coordinate version number.
   **/
  private int _version_number = -1;
  
  /**
   * Creates a new request, ready to be sent out to the network.
   *
   * @param requestor The node orginiating this request.
   * @param coord The requestor's current virtual coordinate.
   **/
  public LocateNodeMsg(NodeId requestor, VirtualCoordinate coord, int version){
    _requestor = requestor;
    _coordinate = coord;
    _version_number = version;
  }
  
  /**
   * Deserializes an existing request from the input buffer (network).
   *
   * @param buffer The buffer from which to retreive the object.
   **/
  public LocateNodeMsg(InputBuffer buffer) throws QSException {

    _version_number = buffer.nextInt();
    _requestor = (NodeId) buffer.nextObject();
    _coordinate = (VirtualCoordinate) buffer.nextObject();
  }

  /**
   * Returns the id of the node who orginally made this request.
   *
   * @return The sender's node id.
   **/
  
  public NodeId getRequestor() {
    return _requestor;
  }

  /**
   * Returns the virtual coordinate of the requestor.
   **/
  
  public VirtualCoordinate getRequestorCoord() {
    return _coordinate;
  }

  /**
   * Returns the virtual coordinate version number of the requestor.
   **/
  public int getVersion() {
    return _version_number;
  }
  
  /**
   * Serializes this object.
   **/
  
  public void serialize (OutputBuffer buffer) {
    buffer.add(_version_number);
    buffer.add(_requestor);
    buffer.add(_coordinate);
  }

  public String toString () {

    return "(VLocateNodeMsg requestor="+ getRequestor() + " coord=" + _coordinate +" vn=" + _version_number+")";
  }
}
