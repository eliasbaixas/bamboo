/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.api;
import java.math.BigInteger;
import ostore.util.QuickSerializable;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SinkIF;

/**
 * BambooRouterAppRegReq.
 *
 * @author  Sean C. Rhea
 * @version $Id: BambooRouterAppRegReq.java,v 1.1 2003/10/05 19:02:02 srhea Exp $
 */
public class BambooRouterAppRegReq implements QueueElementIF {

    public long app_id;
    public boolean send_leaf_sets;
    public boolean send_rt;
    public boolean send_reverse_rt;
    public SinkIF completion_queue;

    public BambooRouterAppRegReq (
	    long a, boolean ls, boolean rt, boolean rrt, SinkIF q) { 
	app_id = a; 
	send_leaf_sets = ls;
	send_rt = rt;
	send_reverse_rt = rrt;
	completion_queue = q;
    }

    public String toString () {
	return "(BambooRouterAppRegReq app_id=" + Long.toHexString (app_id) +
	    " send_leaf_sets=" + send_leaf_sets + 
	    " send_rt=" + send_rt + 
	    " send_reverse_rt=" + send_reverse_rt + ")";
    }
}

