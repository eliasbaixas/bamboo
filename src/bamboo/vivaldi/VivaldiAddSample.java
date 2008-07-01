/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.vivaldi;

import seda.sandStorm.api.QueueElementIF;

/**
 * Adds a latency sample to the Vivaldi's virtual coordinate calculation.
 * A sample is composed of two elements -- the virtual coordinate at the
 * remote node, and the latency measurement between the local and remote nodes.
 *
 * <p>
 *
 * Other stages can dispatch this message to add measurements to the local
 * node's virtual coordinate stored in the Vivaldi stage.
 *
 * @author Steven Czerwinski
 * @version $Id: VivaldiAddSample.java,v 1.1 2004/03/04 21:22:20 czerwin Exp $
 **/

public class VivaldiAddSample implements QueueElementIF {

  /**
   * The virtual coordinate of the remote node.
   **/
  
  public VirtualCoordinate remote_coordinate;

  /**
   * The one way latency betwen the local node and the remote node
   * (milliseconds).
   **/
  public double latency_ms;

  /**
   * Constructs a message containing a single latency/coordinate sample.
   *
   * @param coordinate The current virtual coordinate at the remote node.
   * @param latency The one way latency between the local and remote node in milliseconds.
   **/
  
  public VivaldiAddSample(VirtualCoordinate coordinate, double latency_ms) {
    remote_coordinate = coordinate;
    this.latency_ms = latency_ms;
  }
  
}

