/**
 * Copyright (c) 2002 Regents of the University of California.  All
 * rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials provided
 *    with the distribution.
 * 3. Neither the name of the University nor the names of its
 *    contributors may be used to endorse or promote products derived
 *    from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS
 * IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE
 * REGENTS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
 * EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package bamboo.util;
import bamboo.lss.ASyncCore;
import bamboo.lss.DustDevil;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import ostore.dispatch.Classifier;
import ostore.dispatch.Filter;
import ostore.util.Clock;
import ostore.util.NodeId;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.EventHandlerException;
import seda.sandStorm.api.EventHandlerIF;
import seda.sandStorm.api.NoSuchStageException;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import seda.sandStorm.api.SinkException;
import seda.sandStorm.api.SinkIF;
import seda.sandStorm.api.StageIF;

/**
 * Implements functionality common to most stages.
 *
 * @author  Sean C. Rhea
 * @version $Id: StandardStage.java,v 1.15 2005/05/20 22:24:01 srhea Exp $
 */

public abstract class StandardStage 
implements EventHandlerIF, SingleThreadedEventHandlerIF {

    protected Classifier classifier;
    protected NodeId my_node_id;
    protected SinkIF my_sink;
    protected Class [] event_types = {};
    protected Class [] inb_msg_types = {};
    protected Class [] outb_msg_types = {};

    protected boolean DEBUG;

    // This will be set in all stages or in none, so it's okay to have it be a
    // static variable.
    protected static boolean sim_running;
    protected ASyncCore acore;

    protected Logger logger;

    protected StandardStage () {
        logger = Logger.getLogger (getClass ().getName ());
    }

    protected EventHandlerIF lookup_stage (ConfigDataIF config, String name)
    throws NoSuchStageException {
        StageIF stage = config.getManager ().getStage (name);
        return stage.getWrapper ().getEventHandler ();
    }

    /**
     * Returns the current time in milliseconds; works correctly under the
     * Bamboo simulator and the Simple OceanStore Simulator (SOSS).
     */
    protected long now_ms () {
        return now_ms(my_node_id);
    }

    /**
     * Returns the current time in milliseconds; works correctly under the
     * Bamboo simulator and the Simple OceanStore Simulator (SOSS).
     */
    public static long now_ms (NodeId node_id) {
        return Clock.current_date (node_id).getTime ();
    }

    /**
     * Like now_ms, but uses GetTimeOfDayCC if it's available.
     * Also works correctly under the Bamboo simulator and SOSS.
     */
    protected long timer_ms () {
        return timer_ms(my_node_id);
    }

    /**
     * Like now_ms, but uses GetTimeOfDayCC if it's available.
     * Also works correctly under the Bamboo simulator and SOSS.
     */
    public static long timer_ms (NodeId node_id) {
        if ((! sim_running) && GetTimeOfDayCC.available ())
            return GetTimeOfDayCC.currentTimeMillis ();
        return Clock.current_date (node_id).getTime ();
    }

    /**
     * Like calling assert(false) in C.
     */
    protected final void BUG (String msg) {
        Exception e = null;
        try { throw new Exception (); } catch (Exception c) { e = c; }
        logger.fatal (msg, e);
        System.exit (1);
    }

    /**
     * Like calling assert(false) in C; prints a stack trace.
     */
    protected final void BUG (String msg, Exception e) {
        logger.fatal (msg, e);
        System.exit (1);
    }

    /**
     * Like calling assert(false) in C; prints a stack trace.
     */
    protected final void BUG (Exception e) {
        BUG ("unhandled exception", e);
    }

    protected int configGetInt(ConfigDataIF config, String name, 
                               int defaultValue) {
        int result = config.getInt(name);
        if (result == -1)
            result = defaultValue;
        logger.info("config." + name + "=" + result);
        return result;
    }

    protected double configGetDouble(ConfigDataIF config, String name, 
                                     double defaultValue) {
        double result = config.getDouble(name);
        if (result == -1.0)
            result = defaultValue;
        logger.info("config." + name + "=" + result);
        return result;
    }

    protected int config_get_int (ConfigDataIF config, String name) {
        int result = config.getInt (name);
        logger.info ("config." + name + "=" + result);
        return result;
    }

    protected boolean config_get_boolean (ConfigDataIF config, String name) {
        boolean result = config.getBoolean (name);
        logger.info ("config." + name + "=" + result);
        return result;
    }

    protected String config_get_string (ConfigDataIF config, String name) {
        String result = config.getString (name);
        logger.info ("config." + name + "=" + result);
        return result;
    }

    protected double config_get_double (ConfigDataIF config, String name) {
        double result = config.getDouble (name);
        logger.info ("config." + name + "=" + result);
        return result;
    }

    public void init(ConfigDataIF config) throws Exception {

	int debug_level = config.getInt ("debug_level");

        acore = DustDevil.acore_instance ();
	sim_running = config.getBoolean("simulator_running");

	my_node_id = new NodeId (config.getString ("node_id"));
	classifier = Classifier.getClassifier (my_node_id);

        my_sink = config.getStage().getSink();

	for (int i = 0; i < event_types.length; ++i) {
	    Filter filter = new Filter ();
	    if (! filter.requireType (event_types [i]))
		logger.fatal ("could not require type " +
                        event_types [i].getName ());
            if (logger.isDebugEnabled ())
		logger.debug ("subscribing to " + event_types [i].getName () );
	    classifier.subscribe (filter, my_sink);
	}

	for (int i = 0; i < inb_msg_types.length; ++i) {
	    ostore.util.TypeTable.register_type (inb_msg_types [i]);
	    Filter filter = new Filter ();
	    if (! filter.requireType (inb_msg_types [i]))
		logger.fatal ("could not require type " +
                        inb_msg_types [i].getName ());
	    if (! filter.requireValue ("inbound", new Boolean (true)))
		logger.fatal ("could not require inbound = true for "
			+ inb_msg_types [i].getName ());
            if (logger.isDebugEnabled ())
		logger.debug ("subscribing to " + inb_msg_types [i].getName ());
	    classifier.subscribe (filter, my_sink);
	}

	for (int i = 0; i < outb_msg_types.length; ++i) {
	    Filter filter = new Filter ();
	    if (! filter.requireType (outb_msg_types [i]))
		logger.fatal ("could not require type " +
                        outb_msg_types [i].getName ());
	    if (! filter.requireValue ("inbound", new Boolean (false)))
		logger.fatal ("could not require inbound = false for "
			+ outb_msg_types [i].getName ());
            if (logger.isDebugEnabled ())
		logger.debug ("subscribing to " +
                        outb_msg_types [i].getName ());
	    classifier.subscribe (filter, my_sink);
	}
    }

    public void destroy() {}

    public void handleEvent (QueueElementIF item) {}

    public void handleEvents(QueueElementIF element_array[])
    throws EventHandlerException {
	for (int i = 0; i < element_array.length; ++i)
	    handleEvent(element_array[i]);
    }

    protected final void enqueue (SinkIF sink, QueueElementIF item) {
        if (logger.isDebugEnabled ()) logger.debug ("enqueuing " + item);
	try {
	    sink.enqueue (item);
	}
	catch (SinkException e) {
	    BUG ("could not enqueue " + item, e);
	}
    }

    protected final void dispatch (QueueElementIF item) {
        if (logger.isDebugEnabled ())
            logger.debug ("dispatching " + item + " type " +
                    item.getClass ().getName ());

	try {
	    classifier.dispatch (item);
	}
	catch (SinkException e) {
	    BUG ("could not dispatch " + item, e);
	}
    }
}

