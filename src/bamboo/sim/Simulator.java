/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.sim;
import bamboo.lss.StageManager;
import bamboo.lss.StageWrapper;
import java.io.*;
import java.lang.reflect.Constructor;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import ostore.dispatch.Classifier;
import ostore.util.Clock;
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
import soss.core.SimulatorSink;
import soss.core.SimulatorStage;
import bamboo.lss.DustDevil;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import org.apache.log4j.PropertyConfigurator;

/**
 * The "main" class for the Bamboo simulator.
 *
 * @author Sean C. Rhea
 * @version $Id: Simulator.java,v 1.20 2005/06/30 23:38:49 srhea Exp $
 */
public class Simulator {

    public int node_id_to_graph_index (NodeId node_id) {
        InetAddress addr = node_id.address();
        byte [] bytes = addr.getAddress ();
        assert bytes.length == 4;
        bytes[0] = 0;
        ByteBuffer bb = ByteBuffer.wrap (bytes);
        return bb.getInt ();
    }

    protected static class SimulatorClockCB implements Clock.ClockCB {
	protected EventQueue event_queue;
	public SimulatorClockCB (EventQueue e) { event_queue = e; }
	public Date date (NodeId node_id) {
            return new Date ((event_queue.now_us () + 999) / 1000); // round up
	}
    }

    protected static Logger logger = Logger.getLogger (Simulator.class);
    protected static Simulator instance;

    public static Simulator instance () {
        return instance;
    }

    public static void main (String [] args) throws Exception {
        String cfgfile = null;
	Simulator sim = new Simulator ();

        if (args.length == 3) {
            String logfile = args [1];
            cfgfile = args [2];
            PropertyConfigurator pc = new PropertyConfigurator ();
            pc.configure (logfile);
        }
        else if (args.length == 1) {
            cfgfile = args [0];
            Logger.getRoot ().addAppender (new SimulatorLogAppender ());
            Logger.getRoot ().setLevel (Level.INFO);
        }
        else {
	    System.err.println ("usage: java bamboo.sim.Simulator <cfg file>");
	    System.exit (1);
	}

	sim.run (cfgfile);
    }

    public EventQueue event_queue;
    public NetworkModel network_model;
    public SimulatedASyncCore acore;

    public Simulator () {
        instance = this;
	event_queue = new EventQueue ();

        // Simulate ASyncCore.
        acore = new SimulatedASyncCore(event_queue);
        DustDevil.set_acore_instance (acore);

        // Set the event queue.
        Clock.set_cb (new SimulatorClockCB (event_queue));
    }

    protected class MyClassifierTimerCB implements Classifier.TimerCB {
	protected MyClassifierAlarmCB classifier_alarm_cb;
	protected NodeId node_id;

	public MyClassifierTimerCB (Classifier classifier, NodeId n) {
	    classifier_alarm_cb = new MyClassifierAlarmCB (classifier);
	    node_id = n;
	}
	public Object schedule (long millis, QueueElementIF event) {
            if (logger.isDebugEnabled ())
                logger.debug ("scheduling " + event);
	    return event_queue.register_timer (
                    node_id, millis * 1000, classifier_alarm_cb, event);
	}
	public void cancel (Object token) {
	    event_queue.cancel_timer (token);
	}
    }

    protected class MyClassifierAlarmCB implements EventQueue.Callback {
	protected Classifier classifier;
	public MyClassifierAlarmCB (Classifier c) {
	    classifier = c;
	}
	public void call (Object user_data) {
	    QueueElementIF item = (QueueElementIF) user_data;
            if (logger.isDebugEnabled ())
                logger.debug ("dispatching " + item);
	    try {
		classifier.dispatch (item);
	    }
	    catch (SinkFullException e) {
		System.err.println ("could not dispatch later " + item);
		e.printStackTrace (System.err);
	    }
	}
    }

    protected class NodeCleanupCb implements EventQueue.Callback {
        public NodeId node_id;
        public StageManager mgr;

        public NodeCleanupCb(NodeId n) { node_id = n; }

        public void call(Object notUsed) {
            // Find all the stages.
            LinkedList<String> stagenames = new LinkedList<String>();
            Enumeration allStages = mgr.getConfig().getStages();
            while (allStages.hasMoreElements()) {
                stageDescr descr = (stageDescr)allStages.nextElement();
                stagenames.add(descr.stageName);
            }

            // And destroy them.
            for (String stageName : stagenames) {
                try { mgr.destroyStage(stageName); } 
                catch (Exception e) {
                    logger.warn("Got exception " + e 
                                + " when trying to destroy stage " 
                                + stageName);
                }
            }

            Classifier.removeClassifier(node_id);
        }
    }

    protected class LoadNodeCb implements EventQueue.Callback {
	protected StreamTokenizer tok;
	protected int lineno;
	protected NodeId next_node_id;
	protected String next_cfg_filename;
	protected long next_start_time_us;
	protected long next_stop_time_us;
	// protected int next_graph_index;

	public void call (Object user_data) {

	    // Create the last node we read in.

	    logger.info ("loading " + next_node_id + " from " +
		    next_cfg_filename +  ".  Start time is " +
                    next_start_time_us + " and stop time is " +
                    next_stop_time_us + ".");

            int next_graph_index = node_id_to_graph_index (next_node_id);
            NodeCleanupCb cleanup_cb = new NodeCleanupCb(next_node_id);
	    event_queue.add_node (next_node_id,
		    next_stop_time_us - next_start_time_us,
		    cleanup_cb, null);

	    // Create the classifier.
	    Classifier classifier = Classifier.getClassifier (next_node_id);
	    classifier.set_timer_cb (new MyClassifierTimerCB (
			classifier, next_node_id));

            SandstormConfig cfg = null;
            try {
                // SandStorm never closes the file, so we do it ourselves.
                FileReader reader = new FileReader(next_cfg_filename);
                cfg = new SandstormConfig(reader);
                reader.close();
            }
            catch (IOException e) {
                System.err.println("Error with config file " 
                                   + next_cfg_filename);
                System.err.println("Caught exception " + e + " during parse");
                e.printStackTrace(System.err);
                System.exit(1);
            }

	    StageManager mgr = new StageManager (cfg);
            cleanup_cb.mgr = mgr;

	    // Create the network stage.

	    {
		Hashtable initargs = new Hashtable ();
		initargs.put ("node_id", next_node_id.toString ());
		initargs.put ("simulator_running", "true");
		ConfigData config_data = new ConfigData (mgr, initargs);

                EventHandlerIF network = null;
                try { network = new Network(next_node_id, acore); } 
                catch (IOException e) {
                    logger.fatal("IOException initializing network " + e);
                    System.exit(1);
                }
		SinkIF sink = new SimulatorSink (network);
		StageIF stage = new SimulatorStage (
			"Network", new StageWrapper (network), sink);
		config_data.setStage (stage);
		try {
                    event_queue.set_current_node_id (next_node_id);
		    network.init (config_data);
                    event_queue.set_current_node_id (null);
		}
		catch (Exception e) {
		    System.err.println ("Error with node " + next_node_id + ".");
		    System.err.println ("Caught exception " + e + " during init.");
		    e.printStackTrace (System.err);
		    System.exit (1);
		}
		mgr.addStage (stage.getName (), stage);
	    }

	    // Create the other stages.

	    LinkedList handlers = new LinkedList ();
	    Enumeration iter = cfg.getStages ();
	    while (iter.hasMoreElements ()) {
		stageDescr descr = (stageDescr) iter.nextElement ();
		if (descr.stageName.indexOf ("Network") != -1)
		    continue;

                descr.initargs.put ("simulator_running", "true");

		ConfigData config_data = new ConfigData (mgr, descr.initargs);

		Class c = null;
		try {
		    c = Class.forName (descr.className);
		}
		catch (ClassNotFoundException e) {
		    System.err.println ("Error: could not load class " +
			    descr.className + ".");
		    System.exit (1);
		}

		EventHandlerIF handler = null;
		try {
		    handler = (EventHandlerIF) c.newInstance ();
		}
		catch (Exception e) {
		    System.err.println ("Error: caught exception " +
			    e + " while trying to instantiate " +
			    "an object of type " + descr.className +
			    ".  Stack trace follows:");
		    e.printStackTrace (System.err);
		    System.exit (1);
		}

		SinkIF sink = new SimulatorSink (handler);
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
                    event_queue.set_current_node_id (next_node_id);
		    handler.init (config_data);
                    event_queue.set_current_node_id (null);
		}
		catch (Exception e) {
		    System.err.println ("Error: caught an exception " +
			    e + " while trying to initialize the stage " +
			    handler + " Stack trace follows:");
		    e.printStackTrace (System.err);
		    System.exit (1);
		}
	    }

	    try {
                event_queue.set_current_node_id (next_node_id);
                classifier.enqueue (new StagesInitializedSignal ());
                event_queue.set_current_node_id (null);
	    }
	    catch (SinkException e) {
		System.err.println ("got exception on init: " + e);
		e.printStackTrace (System.err);
		System.exit (1);
	    }

	    // Load another node.

	    load_next_node ();
	}

	void load_next_node () {

	    try {
		// Read in the node's info.
		while (tok.ttype == StreamTokenizer.TT_EOL) {
		    tok.nextToken ();
		    lineno++;
		}

		if (tok.ttype == StreamTokenizer.TT_EOF)
		    return;  // All nodes read.

		// Read in the node count.
		if (tok.ttype != StreamTokenizer.TT_WORD) {
		    System.err.println ("Expected node id on line " +
			    lineno + ": " + tok);
		    System.exit (1);
		}
		next_node_id = new NodeId (tok.sval);
		tok.nextToken ();

		// Read in the cfg file name.
		if (tok.ttype != StreamTokenizer.TT_WORD) {
		    System.err.println ("Expected cfg file name on line " +
			    lineno + ": " + tok);
		    System.exit (1);
		}
		next_cfg_filename = tok.sval;
		tok.nextToken ();

		// Read in the start time.
		if (tok.ttype != StreamTokenizer.TT_WORD) {
		    System.err.println ("Expected start time on line " +
			    lineno + ": " + tok);
		    System.exit (1);
		}
		next_start_time_us = Long.parseLong (tok.sval) * 1000;
		tok.nextToken ();

		// Read in the stop time.
		if (tok.ttype != StreamTokenizer.TT_WORD) {
		    System.err.println ("Expected stop time on line " +
			    lineno + ": " + tok);
		    System.exit (1);
		}
		next_stop_time_us = Long.parseLong (tok.sval) * 1000;
		tok.nextToken ();

		// Read in the graph index.
		/* if (tok.ttype != StreamTokenizer.TT_WORD) {
		    System.err.println ("Expected graph index on line " +
			    lineno + ": " + tok);
		    System.exit (1);
		}
		next_graph_index = Integer.parseInt (tok.sval);
		tok.nextToken (); */
	    }
	    catch (Exception e) {
		System.err.println ("Caught " + e + " parsing line " +
			lineno + " of " + tok);
		System.exit (1);
	    }

	    // Set a callback for the start time.

	    event_queue.register_timer (
		    next_start_time_us - event_queue.now_us (), this, null);
	}
    }

    public void run (String exp_filename) throws Exception {

	FileInputStream is = new FileInputStream (exp_filename);
	Reader reader = new BufferedReader (new InputStreamReader (is));
	StreamTokenizer tok = new StreamTokenizer (reader);
	tok.resetSyntax ();
	tok.whitespaceChars ((int) ' ', (int) ' ');
	tok.whitespaceChars ((int) '\t', (int) '\t');
	tok.wordChars ((int) '-', (int) '-');
	tok.wordChars ((int) '.', (int) '.');
	tok.wordChars ((int) '/', (int) '/');
	tok.wordChars ((int) ':', (int) ':');
	tok.wordChars ((int) 'a', (int) 'z');
	tok.wordChars ((int) 'A', (int) 'Z');
	tok.wordChars ((int) '0', (int) '9');

	int lineno = 1;
	tok.nextToken ();
	while (tok.ttype == StreamTokenizer.TT_EOL) {
	    tok.nextToken ();
	    lineno++;
	}

	// Read in the network model class.
	if (tok.ttype != StreamTokenizer.TT_WORD) {
	    System.err.println ("Expected network model class " +
		    "on line " + lineno + ": " + tok);
	    System.exit (1);
	}
	String network_model_class_name = tok.sval;
	tok.nextToken ();

	// Read in the network model file name.
	if (tok.ttype != StreamTokenizer.TT_WORD) {
	    System.err.println ("Expected network model file name " +
		    "on line " + lineno + ": " + tok);
	    System.exit (1);
	}
	String network_model_file_name = tok.sval;
	tok.nextToken ();

	// Create the network model.
	Class network_model_class = Class.forName (network_model_class_name);

	Class [] args_t = {("").getClass ()};
	Constructor constructor = network_model_class.getConstructor (args_t);

	Object [] args = {network_model_file_name};
	try {
	    network_model = (NetworkModel) constructor.newInstance (args);
	}
	catch (Exception e) {
	    System.err.println ("Caught " + e.getCause () + " calling new " +
		    network_model_class_name + " (" + network_model_file_name +
		    ")");
	    e.getCause ().printStackTrace (System.err);
	    System.exit (1);
	}

	// Start loading nodes.

	LoadNodeCb load_node_cb = new LoadNodeCb ();
	load_node_cb.tok = tok;
	load_node_cb.lineno = lineno;

	load_node_cb.load_next_node ();

	event_queue.run ();

        // logger.info ("simulation finished");
    }
}

