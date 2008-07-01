/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.lss;

import seda.sandStorm.api.EventHandlerIF;
import seda.sandStorm.api.SourceIF;
import seda.sandStorm.api.StageIF;
import seda.sandStorm.api.internal.*;

/**
 * Part of pretending to be SandStorm.
 *
 * @author Sean C. Rhea
 * @version $Id: StageWrapper.java,v 1.4 2003/10/05 18:22:11 srhea Exp $
 */
public class StageWrapper implements StageWrapperIF {

    protected EventHandlerIF eh;
    public StageWrapper (EventHandlerIF eh) { this.eh = eh; }
    public EventHandlerIF getEventHandler() { return eh; }

    public StageIF getStage() { return null; /* TODO */ }
    public SourceIF getSource() { return null; /* TODO */ }
    public ThreadManagerIF getThreadManager() { return null; /* TODO */ }
    public StageStatsIF getStats() { return null; /* TODO */ }
    public ResponseTimeControllerIF getResponseTimeController() { 
	return null; /* TODO */ 
    }
    public BatchSorterIF getBatchSorter() { return null; /* TODO */ }
    public void setBatchSorter(BatchSorterIF sorter) { /* TODO */}
    public void init() throws Exception { /* TODO */}
    public void destroy() throws Exception { /* TODO */}
}

