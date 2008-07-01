/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.api;
import java.util.LinkedList;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SinkIF;

/**
 * BambooLeafSetChanged.
 *
 * @author  Sean C. Rhea
 * @version $Id: BambooLeafSetChanged.java,v 1.1 2003/10/05 19:02:02 srhea Exp $
 */
public class BambooLeafSetChanged implements QueueElementIF {

    public BambooNeighborInfo [] preds;
    public BambooNeighborInfo [] succs;

    public BambooLeafSetChanged (
	    BambooNeighborInfo [] p, BambooNeighborInfo [] s) { 
	preds = p;  
	succs = s;
    }

    public String toString () {
	String result = "(BambooLeafSetChanged preds=(";
	for (int i = preds.length - 1; i >= 0; --i) 
	    result += preds [i] + ((i == 0) ? "" : " ");
	result += ") succs=";
	for (int i = 0; i < succs.length; ++i) 
	    result += succs [i] + ((i == succs.length - 1) ? "" : " ");
	return result + "))";
    }
}

