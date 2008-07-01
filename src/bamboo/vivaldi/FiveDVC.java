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
 * A virtual coordinate class with five dimensions.
 * 
 * @author Steven E. Czerwinski
 * @version $Id: FiveDVC.java,v 1.1 2004/03/04 21:22:20 czerwin Exp $
 */

public class FiveDVC extends VirtualCoordinate implements QuickSerializable {

  public FiveDVC() {
  }

  public FiveDVC(InputBuffer buffer) throws QSException {
    super(buffer);
  }

  protected int getDimensions() {
    return 5;
  }
}
