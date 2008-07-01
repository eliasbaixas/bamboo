/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.vivaldi;

import ostore.util.QuickSerializable;
import ostore.util.InputBuffer;
import ostore.util.QSException;

/**
 * A virtual coordinate class where coordinates are modeled by a position
 * on a plane, and a height away from it.
 * 
 * @author Steven E. Czerwinski
 * @version $Id: SpecialVC.java,v 1.3 2004/03/10 20:39:43 czerwin Exp $
 */

public class SpecialVC extends VirtualCoordinate implements QuickSerializable {


  public SpecialVC() {
  }

  public SpecialVC(InputBuffer buffer) throws QSException {
    super(buffer);
  }
  
  protected int getDimensions() {
    return 3;
  }

  /**
   * Calculates the distance between this coordinate to the given
   * destination.  This will be an estimate of the one-way network
   * latencies between the two nodes in milliseconds.
   *
   * @param destination The coordinate to compare this object against.
   * @return The distance between the coordinates, as expressed by one-way network latency in milliseconds.
   **/

  public double distance(VirtualCoordinate destination) {

    /* Ok, there are two components of distance..
       first, the distance between the point's project into the network
       plane.  (x,y) - (x,y) coordinates.

       second, the distance to go from the network plane to the point,
       at each point.  z + z
    */

    // distance from the two coordinate to/from the network plane.
    return withinPlaneDistance(destination) + toPlaneDistance(destination);
  }

  private double withinPlaneDistance(VirtualCoordinate destination) {
    // distance between the points projected into network plane.

    double plane_dist = 0.0;
    for (int i = 0; i < _coordinate.length-1; i++)
      plane_dist += (_coordinate[i] - destination._coordinate[i]) *
        (_coordinate[i] - destination._coordinate[i]);
    plane_dist = Math.sqrt(plane_dist);
    return plane_dist;
  }

  private double toPlaneDistance(VirtualCoordinate destination) {
    return Math.abs(_coordinate[_coordinate.length-1]) +
      Math.abs(destination._coordinate[_coordinate.length-1]);
  }
  
  public double[] displacement(VirtualCoordinate destination) {

    double normalization = distance(destination);

    if (normalization == 0.0)
      return null;
    
    double[] result = new double[_coordinate.length];

    
    for (int i = 0; i < _coordinate.length-1; i++)
      result[i] = (destination._coordinate[i] - _coordinate[i])/
        normalization;

    result[_coordinate.length-1] = -1.0 * (destination._coordinate[_coordinate.length-1] + _coordinate[_coordinate.length-1])/normalization;
    
    return result;
  }
  

  /**
   * Update this coordinate's position with the given latency sample.
   *
   * @param remote_coord The node's coordinate used to get the latency sample.
   * @param latency The measured network latency (one-way, in milliseconds) to the remote node.
   **/

  public void update(VirtualCoordinate remote_coord, double latency) {
    //                     long time, boolean rt, boolean ls) {

    /* dir = s_c - my_c ; */
    // special case-- if the two coords are the same, pick a random direction
    double[] dir = displacement(remote_coord);

    if (dir == null) {
      dir = random_displacement(); /* a unit vector in random direction*/
    }
    
    // distance from spring's rest position
    double d = remote_coord.distance(this) - latency;

    // update delta, the dampening factor
    _delta -= .025;
    if (_delta < .05)
      _delta = .05;

    // displacement from rest position, with dampening factor, and apply
    for (int i = 0; i < _coordinate.length; i++)
      _coordinate[i] += dir[i]*d*_delta;


    if (_coordinate[_coordinate.length-1] < .5)
      _coordinate[_coordinate.length-1] = 0.5;
  }

}
