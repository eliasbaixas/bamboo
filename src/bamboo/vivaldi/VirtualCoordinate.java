/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.vivaldi;

import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;

/**
 * Virtual network coordinates as calculated by Vivaldi.  The update algorithm
 * is included in this class.
 * 
 * @author Steven E. Czerwinski
 * @version $Id: VirtualCoordinate.java,v 1.3 2004/03/10 20:39:43 czerwin Exp $
 */

public class VirtualCoordinate extends Object
  implements QuickSerializable {

  /**
   * The dampening factor applied to each sample's update.  (From Vivaldi's
   * algorithm.)
   **/ 
  protected double _delta = 1.0;

  protected double[] _coordinate;

  /**
   * Creates an virtual coordinate at the origin of the space.
   *
   * @param dimensions The number of dimensions to have
   **/
  
  protected VirtualCoordinate() {
    _coordinate = new double[getDimensions()];
    for (int i = 0; i < _coordinate.length; i++)
      _coordinate[i] = 0.0;
  }

  /**
   * Returns the number of dimensions to use for this version of the
   * virtual coordinates.
   **/
  
  protected int getDimensions() {
    return 3;
  }
  
  public VirtualCoordinate(InputBuffer buffer) throws QSException {
    _coordinate = new double[getDimensions()];
    for (int i = 0; i < _coordinate.length; i++)
      _coordinate[i] = buffer.nextDouble();
  }

  public void serialize (OutputBuffer buffer) {
    for (int i = 0; i < _coordinate.length; i++)
      buffer.add(_coordinate[i]);
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

    double result = 0.0;
    
    for (int i = 0; i < _coordinate.length; i++)
      result += (_coordinate[i]-destination._coordinate[i]) *
        (_coordinate[i]-destination._coordinate[i]);

    result = Math.sqrt(result);
        
    return result;
  }

  /**
   * Returns a unit vector in the direction towards the remote coordinate.
   * If the coordinates are the same, null is returned since you cannot
   * produce a unit vector.
   *
   * @param remote The remote coordinate.
   * @return A unit vector in the direction towards the remote, or null if the coords are the same.
   **/
  
  protected double[] displacement(VirtualCoordinate remote) {

    double length = distance(remote);

    if (length == 0)
      return null;
    
    double[] dir = new double[_coordinate.length];

    for (int i = 0; i < _coordinate.length; i++)
      dir[i] = (remote._coordinate[i] - _coordinate[i])/length;

    return dir;
  }

  /**
   * Creates a unit vector in a random direction.
   **/
  
  protected double[] random_displacement(double[] dir, int size) {

    if (dir == null)
      dir = new double[size];
    
    double length = 0.0;
    
    for (int i = 0; i < size; i++) {
      dir[i] = Math.random() - 0.5;
      length += dir[i]*dir[i];
    }

    length = Math.sqrt(length);

    // make it a unit vector
    for (int i = 0; i < size; i++)
      dir[i] = dir[i]/length;

    return dir;
  }

  protected double[] random_displacement(int size) {
    return random_displacement(null,size);
  }

  protected double[] random_displacement() {
    return random_displacement(_coordinate.length);
  }
  
  /**
   * Update this coordinate's position with the given latency sample.
   *
   * @param remote_coord The node's coordinate used to get the latency sample.
   * @param latency The measured network latency (one-way, in milliseconds) to the remote node.
   **/
 
  /*  Maybe we need these parameters.. I'm not sure yet.
    @param time The wall clock time when this sample was gathered.
    @param rt True if the remote node is part of the routing table.
    @param ls True if the remote node is part of the leaf set.
   */

  public void update(VirtualCoordinate remote_coord, double latency) {
    //                     long time, boolean rt, boolean ls) {

    /* dir = s_c - my_c ; */
    // special case-- if the two coords are the same, pick a random direction
    double[] dir = displacement(remote_coord);

    if (dir == null)
      dir = random_displacement(); /* a unit vector in random direction*/
    
    // distance from spring's rest position
    double d = remote_coord.distance(this) - latency;

    // update delta, the dampening factor
    _delta -= .025;
    if (_delta < .05)
      _delta = .05;

    // displacement from rest position, with dampening factor, and apply
    for (int i = 0; i < _coordinate.length; i++)
      _coordinate[i] += dir[i]*d*_delta;
  }

  public boolean equals(Object other) {
    if (!(other instanceof VirtualCoordinate) || (other == null))
      return false;

    VirtualCoordinate o = (VirtualCoordinate) other;
    for (int i = 0; i < _coordinate.length; i++)
      if (_coordinate[i] != o._coordinate[i])
        return false;
    return true;
  }

  public String toString() {

    String result = "(";
    for (int i = 0; i < _coordinate.length-1; i++)
      result += (_coordinate[i] + ",");
    result += _coordinate[_coordinate.length-1] + ")";
    return result;
  }

  /**
   * Returns a copy of the coordinate array.
   **/
  
  public double[] getCoordinates() {
    return (double []) _coordinate.clone();
  }
  
}

