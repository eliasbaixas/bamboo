/*
 * Copyright (c) 2005 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.lss;
import java.net.InetSocketAddress;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import ostore.network.NetworkMessage;
import ostore.network.NetworkMessageResult;
import ostore.network.NetworkLatencyReq;
import ostore.network.NetworkLatencyResp;
import ostore.util.CountBuffer;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QuickSerializable;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SinkException;
import seda.sandStorm.api.SinkIF;
import static bamboo.util.Curry.*;

/**
 * Thrown when a second or subsequent handler is registered for a message
 * type.
 *
 * @author Sean C. Rhea
 * @version $Id: DuplicateTypeException.java,v 1.1 2005/03/19 00:42:19 srhea Exp $
 */
public class DuplicateTypeException extends Exception {
    public Class type;
    public DuplicateTypeException (Class t) { type = t; }
    public String toString () { 
        return "DuplicateTypeException: " + type.getName ();
    }
}

