/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.openhash.multicast;
import bamboo.dht.bamboo_key;
import bamboo.dht.bamboo_get_args;
import bamboo.dht.bamboo_get_res;
import bamboo.dht.bamboo_placemark;
import bamboo.dht.bamboo_value;
import bamboo.dht.GatewayClient;
import bamboo.lss.ASyncCore;
import bamboo.util.GuidTools;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.Random;
import org.apache.log4j.Logger;
import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QuickSerializable;
import ostore.util.QSException;
import static bamboo.util.Curry.*;
import static bamboo.openhash.multicast.MulticastClient.*;

public class NattedMulticastClient {

    protected static final Logger logger =
        Logger.getLogger(NattedMulticastClient.class);

    protected static final String APPLICATION = "NattedMCastClient";

    protected int maxLevel;
    protected ASyncCore acore;
    protected TcpMsgChannel channel;
    protected GatewayClient client;
    protected BigInteger id, group;
    protected Random rand;
    protected Thunk1<MulticastMessage> upcall;
    protected LinkedList<QuickSerializable> waiting = 
        new LinkedList<QuickSerializable>();
    protected LinkedList<QuickSerializable> inflight = 
        new LinkedList<QuickSerializable>();

    public NattedMulticastClient(ASyncCore acore, GatewayClient client,
                                 long seed, BigInteger group, int maxLevel,
                                 Thunk1<MulticastMessage> upcall) {
        this.acore = acore;
        this.client = client;
        this.upcall = upcall;
        this.group = group;
        this.maxLevel = maxLevel;
        rand = new Random(seed);
        acore.registerTimer(
                0, new Runnable() { public void run() { connect.run(); }});
    }

    public void sendMsg(QuickSerializable payload) {
        if (channel == null) {
            waiting.addLast(payload);
        }
        else {
            inflight.addLast(payload);
            channel.send(payload);
        }
    }

    protected Runnable connect = new Runnable() {
        public void run() {
            byte noise[] = new byte[20];
            rand.nextBytes(noise);
            id = bytes2bi(noise);
            logger.info("using id 0x" + GuidTools.guid_to_string(id));
            doGet.run(new Integer(maxLevel));
        }
    };

    protected Thunk1<Integer> doGet = new Thunk1<Integer>() {
        public void run(Integer lev) {
            int level = lev.intValue();
            BigInteger key = rendezvous_point(id, group, level);
            bamboo_get_args args = new bamboo_get_args();
            args.application = APPLICATION;
            args.key = new bamboo_key();
            args.key.value = bi2bytes(key);
            args.maxvals = Integer.MAX_VALUE;
            args.placemark = new bamboo_placemark();
            args.placemark.value = new byte[0];

            client.get(args, curry(getDone, new Integer(level - 1)));
        }
    };

    protected Thunk2<Integer,bamboo_get_res> getDone = 
        new Thunk2<Integer,bamboo_get_res>() {
        public void run(Integer lev, bamboo_get_res result) {
            int level = lev.intValue();
            if (result.values.length > 0) {
                // We found a parent.  Connect and send all waiting messages.
                int which = rand.nextInt(result.values.length);
                InetSocketAddress udp = 
                    bytes2addr(result.values [which].value);
                InetSocketAddress parent = new InetSocketAddress(
                        udp.getAddress(), udp.getPort() + 1);
                logger.info("trying parent " 
                            + parent.getAddress().getHostAddress() + ":"
                            + parent.getPort());
                channel = new TcpMsgChannel(acore, parent);
                channel.setReceiveHandler(recv);
                channel.setFailureHandler(fail);
                Group msg = new Group(group);
                inflight.addFirst(msg);
                channel.send(msg);
            }
            else if (level < 0) {
                // We're the only client in the tree.  Try again later.
                logger.info("no parents available");
                acore.registerTimer(60*1000, connect);
                return;
            }
            else {
                // Recurse up the tree.
                doGet.run(new Integer(level-1));
            }
        }
    };

    protected Runnable fail = new Runnable() {
        public void run() {
            logger.info("connection failed");
            if (!inflight.isEmpty() && (inflight.getFirst() instanceof Group)) {
                inflight.removeFirst();
            }
            for (QuickSerializable msg : waiting)
                inflight.addLast(msg);
            waiting = inflight;
            inflight = new LinkedList<QuickSerializable>();
            channel = null;
            acore.registerTimer(1000, connect);
        }
    };

    protected Thunk1<QuickSerializable> recv = new Thunk1<QuickSerializable>() {
        public void run(QuickSerializable msg) {
            if (msg instanceof Ack) {
                QuickSerializable first = inflight.removeFirst();
                if (first instanceof Group) {
                    // Our new parent is up.  Send all waiting messages.
                    logger.info("successful join");
                    assert inflight.isEmpty();
                    for (QuickSerializable m : waiting)
                        channel.send(m);
                    inflight = waiting;
                    waiting = new LinkedList<QuickSerializable>();
                }
            }
            else if (msg instanceof MulticastMessage) {
                upcall.run((MulticastMessage) msg);
            }
            else {
                logger.warn("unknown msg type: " + msg.getClass().getName());
            }
        }
    };

    public static class Ack implements QuickSerializable {
        public Ack() {}
        public Ack(InputBuffer buffer) {}
        public void serialize(OutputBuffer buffer) {}
        public Object clone(Object other) throws CloneNotSupportedException { 
            return super.clone(); 
        }
        public String toString() { return "Ack"; }
    }

    public static class Group implements QuickSerializable {
        public BigInteger group;
        public Group(BigInteger g) { group = g; }
        public Group(InputBuffer buffer) throws QSException { 
            group = buffer.nextBigInteger(); 
        }
        public void serialize(OutputBuffer buffer) { buffer.add(group); }
        public Object clone(Object other) throws CloneNotSupportedException { 
            Group result = (Group) super.clone(); 
            result.group = group;
            return result;
        }
        public String toString() { 
            return "Group-0x" + GuidTools.guid_to_string(group); 
        }
    }

    static {
        try {
            ostore.util.TypeTable.register_type(Ack.class);
            ostore.util.TypeTable.register_type(Group.class);
            ostore.util.TypeTable.register_type(MulticastMessage.class);
        }
        catch (Exception e) { assert false : e; }
    }
}

