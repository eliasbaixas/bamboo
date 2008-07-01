/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.www;
import java.util.Iterator;
import java.util.LinkedList;
import org.apache.log4j.*;
import org.apache.log4j.spi.*;

/**
 * For displaying logging messages on the web interface.
 *
 * @author Sean C. Rhea
 * @version $Id: WebAppender.java,v 1.4 2004/10/24 02:28:38 srhea Exp $
 */
public class WebAppender implements Appender {

    protected int max_events;
    protected Layout layout;
    protected String name;
    protected LinkedList events = new LinkedList ();

    public WebAppender (int m) {
        max_events = m;
    }

    public void addFilter (Filter f) {
        throw new NoSuchMethodError ();
    }

    public void clearFilters () {
        throw new NoSuchMethodError ();
    }

    public void close () {
        throw new NoSuchMethodError ();
    }

    public synchronized void doAppend (LoggingEvent e) {
        events.addLast (e);
        if (events.size () > max_events)
            events.removeFirst ();
    }

    public ErrorHandler getErrorHandler () {
        throw new NoSuchMethodError ();
    }

    public Filter getFilter () {
        throw new NoSuchMethodError ();
    }

    public Layout getLayout () {
        return layout;
    }

    public String getName () {
        return name;
    }

    public boolean requiresLayout () { 
        return true;
    }

    public void setErrorHandler (ErrorHandler e) {
        throw new NoSuchMethodError ();
    }

    public void setLayout (Layout l) {
        layout = l;
    }

    public void setName (String n) {
        name = n;
    }

    public synchronized void printEvents (StringBuffer result) {
        for (Iterator i = events.iterator (); i.hasNext (); ) {
            LoggingEvent e = (LoggingEvent) i.next ();
            result.append (layout.format (e));
        }
    }
}
