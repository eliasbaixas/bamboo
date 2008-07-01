/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.lss;
import java.io.IOException;
import java.io.Reader;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.LinkedList;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.PropertyConfigurator;
import ostore.dispatch.Classifier;
import ostore.util.NodeId;
import seda.sandStorm.api.EventHandlerIF;
import seda.sandStorm.api.ManagerIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SinkException;
import seda.sandStorm.api.SinkFullException;
import seda.sandStorm.api.SinkIF;
import seda.sandStorm.api.StageIF;
import seda.sandStorm.api.StagesInitializedSignal;
import seda.sandStorm.internal.ConfigData;
import seda.sandStorm.main.SandstormConfig;
import seda.sandStorm.main.stageDescr;
import soss.core.SimulatorStage;

/**
 * Loads a SandStorm .cfg file, creates all the stages, replaces the network
 * stage with bamboo.lss.Network, sends each stage a StagesInitializedSignal
 * event, and then calls ASyncCore.amain ().  Largely adopted from SOSS code.
 *
 * @author Sean C. Rhea
 * @version $Id: DustDevil.java,v 1.24 2005/04/27 20:51:10 srhea Exp $
 */
public class DustDevil {

    public static ASyncCore acore_instance () {
        return acore;
    }

    public static void set_acore_instance (ASyncCore value) {
        acore = value;
    }

    protected Logger logger;
    public DustDevil () {
        logger = Logger.getLogger (this.getClass ());
    }

    protected class MyClassifierTimerCB implements Classifier.TimerCB {
	protected MyClassifierAlarmCB classifier_alarm_cb;

	public MyClassifierTimerCB (Classifier classifier) {
	    classifier_alarm_cb = new MyClassifierAlarmCB (classifier);
	}
	public Object schedule (long millis, QueueElementIF event) {
	    return acore.register_timer (millis, classifier_alarm_cb, event);
	}
	public void cancel (Object token) { 
	    acore.cancel_timer (token);
	}
    }

    protected class MyClassifierAlarmCB implements ASyncCore.TimerCB {
	protected Classifier classifier;
	public MyClassifierAlarmCB (Classifier c) {
	    classifier = c;
	}
	public void timer_cb (Object user_data) {
	    QueueElementIF item = (QueueElementIF) user_data;
	    try {
		classifier.dispatch (item);
	    }
	    catch (SinkFullException e) {
                logger.error ("could not dispatch later " + item, e);
	    }
	}
    }

    protected static ASyncCore acore;

    public void init_nodes (SandstormConfig config, StageManager mgr) {

	LinkedList handlers = new LinkedList ();
	Enumeration iter = config.getStages ();
	while (iter.hasMoreElements ()) {
	    stageDescr descr = (stageDescr) iter.nextElement ();
	    if (descr.stageName.indexOf ("Network") != -1)
		continue;

	    ConfigData config_data = new ConfigData (mgr, descr.initargs);

	    Class c = null;
	    try {
		c = Class.forName (descr.className);
	    }
	    catch (ClassNotFoundException e) {
		logger.fatal ("Error: could not load class " +
			descr.className + ".");
		System.exit (1);
	    }

	    EventHandlerIF handler = null;
	    try {
		handler = (EventHandlerIF) c.newInstance ();
	    }
	    catch (Exception e) {
		logger.fatal ("Error: caught exception " +
			e + " while trying to instantiate " +
			"an object of type " + descr.className + ".", e);
		System.exit (1);
	    }

	    SinkIF sink = new DustDevilSink (handler, Thread.currentThread ());
	    StageIF stage = new SimulatorStage (
		    descr.stageName, new StageWrapper (handler), sink);
	    config_data.setStage (stage);
	    mgr.addStage (descr.stageName, stage);

	    Object [] p = {handler, config_data};
	    handlers.addLast (p);
	}

	while (! handlers.isEmpty ()) {
	    Object [] p = (Object []) handlers.removeFirst ();
	    EventHandlerIF handler = (EventHandlerIF) p [0];
	    ConfigData config_data = (ConfigData) p [1];

	    try {
		handler.init (config_data);
	    }
	    catch (Exception e) {
		logger.fatal ("Error: caught an exception " +
			e + " while trying to initialize the stage " +
			handler + ".", e);
		System.exit (1);
	    }
	}
    }

    public EventHandlerIF create_network (InetSocketAddress addr) throws IOException {
        return new Network (addr, acore);
    }

    public Classifier.TimerCB create_timer_cb (Classifier classifier) {
        return new MyClassifierTimerCB (classifier);
    }

    public StageManager main (Reader reader) throws Exception { 

	SandstormConfig cfg = null;
	try {
	    cfg = new SandstormConfig (reader);
	}
	catch (IOException e) {
	    logger.fatal ("Error with config file from reader.  " + 
                    "Caught exception " + e + " during parse", e);
	    System.exit (1);
	}

        return main (cfg);
    }

    public StageManager main (String cfg_filename) throws Exception { 

	SandstormConfig cfg = null;
	try {
	    cfg = new SandstormConfig (cfg_filename);
	}
	catch (IOException e) {
	    logger.fatal ("Error with config file from reader.  " + 
                    "Caught exception " + e + " during parse", e);
	    System.exit (1);
	}

        return main (cfg);
    }

    protected void create_network_stage (String name, String node_id_str, 
	    StageManager mgr, ConfigData config_data, LinkedList classifiers) 
	    throws Exception {

	NodeId node_id = null;
	try {
	    node_id = new NodeId (node_id_str);
	}
	catch (ostore.util.NodeId.BadFormat e) {
	    logger.fatal ("node_id must be an IP:port tuple");
	    System.exit (1);
	}
	catch (UnknownHostException e) {
	    logger.fatal ("unknown host: " + node_id_str);
	    System.exit (1);
	}

	Classifier classifier = Classifier.getClassifier (node_id); 
	classifier.set_timer_cb (create_timer_cb (classifier));
	classifiers.addLast (classifier);

	// Create network "stage".

	InetSocketAddress my_addr = new InetSocketAddress (
		node_id.address (), node_id.port ());

	EventHandlerIF network = create_network (my_addr);
	SinkIF sink = new DustDevilSink (network, Thread.currentThread ());
	StageIF stage = new SimulatorStage (
		name, new StageWrapper (network), sink);
	config_data.setStage (stage);
	network.init (config_data);
	mgr.addStage (stage.getName (), stage);
    }

    public StageManager main (SandstormConfig cfg) throws Exception { 

	logger.info ("DustDevil version 0.1");

	// First off, we need to find the network stage so that we can get our
	// hostname and port.

	LinkedList classifiers = new LinkedList ();
	
	StageManager mgr = new StageManager (cfg);
	Enumeration iter = cfg.getStages ();
	while (iter.hasMoreElements ()) {
	    stageDescr descr = (stageDescr) iter.nextElement ();
	    if (descr.stageName.indexOf ("YANetwork") != -1) {
		ConfigData config_data = new ConfigData (mgr, descr.initargs);
		int cnt = config_data.getInt ("node_id_count");
		for (int i = 0; i < cnt; ++i) {
		    String nidstr = config_data.getString ("node_id_" + i);
		    java.util.Hashtable initargs = new java.util.Hashtable ();
		    initargs.put ("node_id", nidstr);
		    initargs.put ("debug_level", "0");
		    ConfigData config_data_tmp = new ConfigData (mgr, initargs);
		    create_network_stage ("Network-"+i, nidstr, mgr, 
			    config_data_tmp, classifiers);
		}
	    }
	    else if (descr.stageName.indexOf ("Network") != -1) {
		ConfigData config_data = new ConfigData (mgr, descr.initargs);
		String nidstr = config_data.getString ("node_id");
		if (nidstr == null) {
		    logger.fatal ("Network stage must have node_id");
		    System.exit (1);
		}
		create_network_stage (descr.stageName, nidstr, mgr, 
			config_data, classifiers);
	    }
	}
	if( classifiers.isEmpty() ) {
	    // No Network stage, but we still need a classifier.
	    String nidstr = cfg.getString ("global.initargs.node_id");
	    NodeId node_id = null;
	    try {
		node_id = new NodeId (nidstr);
	    }
	    catch (ostore.util.NodeId.BadFormat e) {
		logger.fatal ("node_id must be an IP:port tuple");
		System.exit (1);
	    }
	    catch (UnknownHostException e) {
		logger.fatal ("unknown host: " + nidstr);
		System.exit (1);
	    }
	    Classifier classifier = Classifier.getClassifier (node_id); 
	    classifier.set_timer_cb (create_timer_cb (classifier));
	    classifiers.addLast (classifier);
	}
	         
	// Create the other stages.

	init_nodes (cfg, mgr);

	// Send a StagesInitializedSignal.

	while (! classifiers.isEmpty ()) {
	    Classifier classifier = (Classifier) classifiers.removeFirst ();
	    try {
		classifier.enqueue (new StagesInitializedSignal ());
	    }
	    catch (SinkException e) {
		logger.fatal ("got exception on init: " + e, e);
		System.exit (1);
	    }
	}

	System.out.println ("Sandstorm: Ready");
	return mgr;
    }

    public static void usage () {
        System.err.println ("usage: [-l <log prefix>] <config file>");
        System.exit (1);
    }

    public static void main (String [] args) throws Exception {

        String cfgfile = null;

        if (args.length == 3) {
            if (! args [0].equals ("-l")) 
                usage ();
            String logfile = args [1];
            cfgfile = args [2];

            PropertyConfigurator pc = new PropertyConfigurator ();
            pc.configureAndWatch (logfile, 10*1000 /* check every 10 s */);
        }
        else if (args.length == 1) {
            cfgfile = args [0];
        }
        else
            usage ();

        Logger l = Logger.getLogger (DustDevil.class);
        try {
            set_acore_instance (new ASyncCoreImpl ());
        }
        catch (IOException e) {
            l.fatal ("could not open selector", e);
            System.exit (1);
        }

        DustDevil dd = new DustDevil ();
	dd.main (cfgfile);

	// Start the main loop.
	
        try {
            acore_instance ().async_main ();
        }
        catch (OutOfMemoryError e) {
            DustDevilSink.reserve = null;
            System.gc ();
            l.fatal ("uncaught error", e);
            System.exit (1);
        }
        catch (Throwable e) {
            l.fatal ("uncaught exception", e);
            System.exit (1);
        }
    }
}

