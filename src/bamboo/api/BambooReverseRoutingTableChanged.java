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
 * BambooReverseRoutingTableChanged.
 *
 * @author  Sean C. Rhea
 * @version $Id: BambooReverseRoutingTableChanged.java,v 1.1 2003/10/05 19:02:02 srhea Exp $
 */
public class BambooReverseRoutingTableChanged implements QueueElementIF {

    public BambooNeighborInfo [] added;
    public BambooNeighborInfo [] removed;

    public BambooReverseRoutingTableChanged (
	    BambooNeighborInfo [] a, BambooNeighborInfo [] r) { 
	added = a;
	removed = r;
    }

    public String toString () {
	String result = "(BambooReverseRoutingTableChanged added=(";
	if (added != null) {
	    for (int i = added.length - 1; i >= 0; --i) 
		result += added [i] + ((i == 0) ? "" : " ");
	}
	result += ") removed=(";
	if (removed != null) {
	    for (int i = removed.length - 1; i >= 0; --i) 
		result += removed [i] + ((i == 0) ? "" : " ");
	}
	return result + "))";
    }
}

