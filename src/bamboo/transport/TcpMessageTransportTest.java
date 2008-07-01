/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.transport;
import java.io.IOException;
import java.net.UnknownHostException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import org.apache.log4j.Level;
import ostore.util.CountBuffer;
import ostore.util.NodeId;
import ostore.util.QSException;
import ostore.util.QuickSerializable;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import seda.sandStorm.api.StageIF;
import bamboo.lss.ASyncCore;
import bamboo.lss.DustDevil;
import bamboo.lss.NioMultiplePacketInputBuffer;
import bamboo.lss.NioOutputBuffer;
import ostore.util.QSString;

/**
 * A test for the TcpMessageTransport.
 *
 * @author Sean C. Rhea
 * @version $Id: TcpMessageTransportTest.java,v 1.3 2004/04/08 22:08:11 srhea Exp $
 */
public class TcpMessageTransportTest extends bamboo.util.StandardStage 
implements SingleThreadedEventHandlerIF {

    protected int message_num;
    protected InetSocketAddress dst;
    protected TcpMessageTransport transport;

    public class SendCb implements ASyncCore.TimerCB {
        public void timer_cb (Object user_data) {
            ++message_num;
            String msg = "This is message " + message_num;
            QSString qsmsg = new QSString (msg);
            logger.info ("sending " + qsmsg);
            transport.send (dst, qsmsg, new SendDoneCb (), null);
        }
    }
    
    public class SendDoneCb implements TcpMessageTransport.SendDoneCb {
        public void send_done_cb (Object user_data) {
            acore.register_timer (1000, new SendCb (), null);
        }
    }

    public class RecvCb implements TcpMessageTransport.RecvMsgCb {
        public void recv_msg_cb (InetSocketAddress src, QuickSerializable msg) {
            logger.info ("received " + msg);
        }
    }

    public void init (ConfigDataIF config) throws Exception {
	super.init (config);
        String transport_stg_name = 
            config_get_string (config, "transport_stage_name");
        transport = (TcpMessageTransport) 
            lookup_stage (config, transport_stg_name);
        boolean sender = config_get_boolean (config, "sender");
        if (sender) {
            NodeId d = new NodeId (config_get_string (config, "dst"));
            dst = new InetSocketAddress (d.address (), d.port ());
            acore.register_timer (1000, new SendCb (), null);
        }
        else {
            transport.register_recv_cb (QSString.class, new RecvCb ());
        }
    }

    public void handleEvent (QueueElementIF item) {
        BUG ("unexpected event: " + item);
    }
}

