/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.sim;
import org.apache.log4j.*;
import org.apache.log4j.spi.*;
import java.text.NumberFormat;
import ostore.util.NodeId;

public class SimulatorLogAppender extends AppenderSkeleton {

    protected NumberFormat time_format;
    protected EventQueue event_queue;

    public SimulatorLogAppender () {
        event_queue = Simulator.instance ().event_queue;
        time_format = NumberFormat.getInstance();
        time_format.setMinimumFractionDigits(3);
        time_format.setMaximumFractionDigits(3);
        time_format.setMaximumIntegerDigits(6);
        time_format.setMinimumIntegerDigits(6);
        time_format.setGroupingUsed (false);
    }

    public void close () {}

    public boolean requiresLayout () { return false; }

    protected void append (LoggingEvent event) {
        String logger = event.getLoggerName ();
        String msg = event.getRenderedMessage ();
        StringBuffer buf = new StringBuffer (
                logger.length () + msg.length () + 40);
        long now_us = event_queue.now_us ();
        if (now_us == -1)
            buf.append ("XXXXXX.XXX");
        else
            buf.append (time_format.format (now_us / 1000000.0));
        buf.append (" ");
        NodeId node_id = event_queue.current_node_id ();
        if (node_id == null) {
            buf.append ("SIMULATOR: ");
        }
        else {
            buf.append (event_queue.current_node_id ());
            buf.append (" ");
            buf.append (logger);
            buf.append (": ");
        }
        buf.append (msg);
        System.out.println (buf);
    }
}

