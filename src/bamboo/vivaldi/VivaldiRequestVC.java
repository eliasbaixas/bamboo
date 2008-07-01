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
 * Requests the current virtual coordinate for the local node.  Other
 * stages may use this to query the Vivaldi stage.
 *
 * <p>
 * A {@link VivaldiReplyVC} will be sent to the specified sink in response.
 *
 * @author Steven Czerwinski
 * @version $Id: VivaldiRequestVC.java,v 1.1 2004/03/04 21:22:20 czerwin Exp $
 **/

public class VivaldiRequestVC implements QueueElementIF {

  /**
   * The sink to send the resulting {@link VivaldiReplyVC} to.
   **/
  
  public SinkIF comp_q;

  /**
   * A callback object to include with the reply.
   **/
  public Object user_data;
    
  /**
   * Constructs the request.
   *
   * @param completion_queue The sink on which to publish the result.
   * @param user_data A user-specified object to include in the result.
   **/
  
  public VivaldiRequestVC(SinkIF completion_queue, Object user_data) {
    comp_q = completion_queue;
    this.user_data = user_data;
  }
  
}

