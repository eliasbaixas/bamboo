/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.openhash.multicast;
import bamboo.lss.ASyncCore;
import bamboo.util.GuidTools;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import org.apache.log4j.Logger;
import ostore.util.NodeId;
import ostore.util.QuickSerializable;
import static bamboo.util.Curry.*;
import static bamboo.openhash.multicast.MulticastClient.*;
import static bamboo.openhash.multicast.NattedMulticastClient.*;

public class NattedMulticastParent {

    protected static final Logger logger =
        Logger.getLogger(NattedMulticastParent.class);

    protected ASyncCore acore;
    protected TcpMsgServer server;
    protected LinkedHashMap<BigInteger,LinkedHashSet<TcpMsgChannel>> children =
        new LinkedHashMap<BigInteger,LinkedHashSet<TcpMsgChannel>>();
    protected Thunk1<MulticastMessage> upcall;
    protected LinkedHashMap<TcpMsgChannel,BigInteger> groups = 
        new LinkedHashMap<TcpMsgChannel,BigInteger>();

    public NattedMulticastParent(ASyncCore acore, int port, 
            Thunk1<MulticastMessage> upcall) throws IOException {
        this.acore = acore;
        this.upcall = upcall;
        TcpMsgServer server = new TcpMsgServer(acore, port, newChild);
    }

    public void addGroup(BigInteger group) {
        if (!children.containsKey(group))
            children.put(group, new LinkedHashSet<TcpMsgChannel>());
    }

    public void removeGroup(BigInteger group) {
        if (children.containsKey(group)) {
            for (TcpMsgChannel c : children.get(group)) {
                c.close();
                groups.remove(c);
            }
            children.remove(group);
        }
    }

    public void sendMsg(MulticastMessage msg) {
        for (TcpMsgChannel c : children.get(msg.group))
            c.send(msg);
    }

    protected Thunk1<TcpMsgChannel> newChild = new Thunk1<TcpMsgChannel>() {
        public void run(TcpMsgChannel channel) {
            logger.info("new connection from " 
                        + channel.peer().getAddress().getHostAddress() 
                        + ":" + channel.peer().getPort());
            channel.setReceiveHandler(curry(recv, channel));
            // TODO: set a timer in case they never send us a Group message.
        }
    };

    protected Thunk2<TcpMsgChannel,QuickSerializable> recv = 
        new Thunk2<TcpMsgChannel,QuickSerializable>() {
        public void run(TcpMsgChannel channel, QuickSerializable qs) {
            if (logger.isDebugEnabled())
                logger.debug("received " + qs + " from " + channel.peer());
            if (qs instanceof Group) {
                Group g = (Group) qs;
                if (children.containsKey(g.group)) {
                    groups.put(channel, g.group);
                    children.get(g.group).add(channel);
                    channel.setFailureHandler(curry(fail, g.group, channel));
                }
                else {
                    logger.warn("unknown group 0x" 
                            + GuidTools.guid_to_string(g.group)
                            + ".  Closing connection.");
                    channel.close();
                    return;
                }
            }
            else {
                BigInteger group = groups.get(channel);
                if (group == null) {
                    logger.warn("no group set for " + channel.peer() 
                            + ".  Closing connection.");
                    channel.close();
                    return;
                }
                // Create a multicast message.
                MulticastMessage mm = new MulticastMessage(group, 
                        NodeId.create(channel.peer()), 0, 0, qs);
                // Send it out on the public tree.
                upcall.run(mm);
                // And send it to all of our children.
                for (TcpMsgChannel c : children.get(group)) {
                    if (!c.peer().equals(channel.peer()))
                        c.send(mm);
                }
            }
            channel.send(new Ack());
        }
    };

    protected Thunk2<BigInteger,TcpMsgChannel> fail = 
        new Thunk2<BigInteger,TcpMsgChannel>() {
        public void run(BigInteger group, TcpMsgChannel channel) {
            logger.info("connection to " 
                        + channel.peer().getAddress().getHostAddress() 
                        + ":" + channel.peer().getPort() + " closed.");
            children.get(group).remove(channel);
            groups.remove(channel);
        }
    };

    static {
        try {
            ostore.util.TypeTable.register_type(Ack.class);
            ostore.util.TypeTable.register_type(Group.class);
        }
        catch (Exception e) { assert false : e; }
    }
}

