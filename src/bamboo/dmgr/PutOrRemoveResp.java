/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dmgr;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SinkIF;

/**
 * Notify the requesting stage that a PutOrRemoveReq has completed.
 * 
 * @author Sean C. Rhea
 * @version $Id: PutOrRemoveResp.java,v 1.3 2003/10/05 18:22:11 srhea Exp $
 */
public class PutOrRemoveResp implements QueueElementIF {

    public Object user_data;

    public PutOrRemoveResp (Object u) {
        user_data = u;
    }

    public String toString () {
	return "(PutOrRemoveResp user_data=" + user_data + ")";
    }
}

