/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.vivaldi;

import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SinkIF;

/**
 * The reply message sent giving the local node's current virtual coordinate.
 * This is sent in response to a {@link VivaldiRequestVC} message.
 *
 *
 * @author Steven Czerwinski
 * @version $Id: VivaldiReplyVC.java,v 1.1 2004/03/04 21:22:20 czerwin Exp $
 **/

public class VivaldiReplyVC implements QueueElementIF {

  /**
   * The callback object as specified by the {@link VivaldiRequestVC}.
   **/
  public Object user_data;

  /**
   * The current value of the local node's virtual coordinate.
   **/
  
  public VirtualCoordinate coordinate;

  /**
   * The number of latency samples that have been used to calculate this
   * virtual coordinate.
   **/
  
  public int samples;
  
  /**
   * Constructs the reply.
   *
   * @param coordinate The virtual coordinate.
   * @param samples The number of samples used to calculate the coordinate.
   * @param user_data The user-specified object included in the request.
   **/
  
  public VivaldiReplyVC(VirtualCoordinate coordinate, int samples,
                        Object user_data) {
    this.coordinate = coordinate;
    this.samples = samples;
    this.user_data = user_data;
  }
  
}

