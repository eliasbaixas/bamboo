/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.lss;
import java.util.HashMap;
import java.util.Map;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.EventHandlerIF;
import seda.sandStorm.api.ManagerIF;
import seda.sandStorm.api.NoSuchStageException;
import seda.sandStorm.api.ProfilerIF;
import seda.sandStorm.api.SandstormConfigIF;
import seda.sandStorm.api.SignalMgrIF;
import seda.sandStorm.api.SinkException;
import seda.sandStorm.api.SinkFullException;
import seda.sandStorm.api.SinkIF;
import seda.sandStorm.api.StageIF;
import seda.sandStorm.api.StagesInitializedSignal;
import seda.sandStorm.internal.ConfigData;
import soss.core.SimulatorSink;
import soss.core.SimulatorStage;

/**
 * A manager for DustDevil.  Adapted from the one in SOSS to allow for
 * dynamic creation of stages.
 * 
 * @author Sean C. Rhea
 * @version $Id: StageManager.java,v 1.5 2005/07/01 00:06:33 srhea Exp $
 */
public class StageManager implements ManagerIF {

    public StageManager (SandstormConfigIF config) {
	_config = config;
	_stages = new HashMap ();
    }

    public void addStage(String stagename, StageIF stage) {
	_stages.put (stagename, stage);
    }

    public StageIF getStage(String stagename) throws NoSuchStageException {
	StageIF result = (StageIF) _stages.get (stagename);
	if (result == null)
	    throw new NoSuchStageException ();
	return result;
    }

    public StageIF createStage(String stagename, EventHandlerIF handler,
	    String initargs[]) throws Exception {

	SinkIF sink = new SimulatorSink (handler);
	StageIF stage = new SimulatorStage (
		stagename, new StageWrapper (handler), sink);
	addStage (stagename, stage);

	ConfigData config_data = new ConfigData (this, initargs);
	config_data.setStage (stage);
	try {
	    handler.init (config_data);
	}
	catch (Exception e) {
	    System.err.println ("Error: caught an exception " +
		    e + " while trying to initialize the stage " +
		    handler + " Stack trace follows:");
	    e.printStackTrace (System.err);
	    System.exit (1);
	}

	return stage;
    }

    /**
     * Returns a handle to the system signal interface.
     */
    public SignalMgrIF getSignalMgr() {
	throw new IllegalArgumentException ();
    }

    /**
     * Returns a handle to the system profiler. 
     */
    public ProfilerIF getProfiler() {
	throw new IllegalArgumentException ();
    }

    /**
     * Returns a copy of the SandstormConfigIF for this Manager. This
     * contains all of the global options used by the runtime system. Note
     * that modifying any options of this copy does not in fact change the
     * runtime parameters of the system; this is used for informational
     * purposes only.
     */
    public SandstormConfigIF getConfig() {
	return _config;
    }

    /**
     * Note that calling this slightly violates the semantics of this
     * method as defined in ManagerIF.  It does not remove all references
     * to the stage, because the soss Main stage still has a reference
     * to the node's classifier, which contains references to the sink,
     * which has a reference to the EventHandler itself. So the stage won't
     * completely disappear until the reference to the classifier is
     * destroyed (see soss.core.Main.removeNode)
     */
    public void destroyStage(String stagename) throws Exception {
	if (stagename == null) 
	    throw new NoSuchStageException("no such stage: null");
	StageIF stage = (StageIF)_stages.get(stagename);
	if (stage == null) 
	    throw new NoSuchStageException("no such stage: "+stagename);

	// destroy and remove this sucker
	_stages.remove( stagename );
	stage.destroy();
    }

    private Map _stages;
    private SandstormConfigIF _config;
}
