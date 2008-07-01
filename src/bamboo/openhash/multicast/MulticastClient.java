/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.openhash.multicast;
import bamboo.dht.*;
import bamboo.lss.ASyncCore;
import bamboo.lss.DustDevil;
import bamboo.lss.Network;
import bamboo.lss.PriorityQueue;
import bamboo.util.GuidTools;
import bamboo.util.StandardStage;
import java.lang.Integer;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Random;
import org.apache.log4j.Level;
import ostore.network.NetworkMessage;
import ostore.network.NetworkMessageResult;
import ostore.util.ByteUtils; // for print_bytes function only
import ostore.util.NodeId;
import ostore.util.QuickSerializable;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import seda.sandStorm.api.StageIF;
import static bamboo.util.Curry.*;

/**
 * An implementation of multicast.
 *
*/
public class MulticastClient extends StandardStage {

    protected static BigInteger TWO = BigInteger.valueOf (2);
    protected static BigInteger MOD = (BigInteger.valueOf (2)).pow (160);

    protected int next_msg_num;
    protected int next_msg_num() {
        if (++next_msg_num < 0)
            next_msg_num = 0;
        return next_msg_num;
    }

    protected LinkedHashMap<Class,Thunk1<MulticastMessage>> receivers = 
        new LinkedHashMap<Class,Thunk1<MulticastMessage>>();

    /**
     * The given callback will be called every time a new message with a
     * payload of the given type is received.
     * 
     * @throws NoSuchMethodException if the type passed in does not have a
     * constructor that takes an ostore.util.InputBuffer (i.e., it is not a
     * well formed object of type ostore.util.QuickSerializable).
     */
    public <T extends QuickSerializable> void registerReceiver(
            Class<T> type, Thunk1<MulticastMessage> cb) 
        throws NoSuchMethodException {

        if (receivers.containsKey(type)) {
            throw new IllegalStateException("already have a receiver of type " 
                    + type.getName() + ": " 
                    + receivers.get(type).getClass().getName());
        }
        try { ostore.util.TypeTable.register_type(type); }
        catch (ostore.util.TypeTable.DuplicateTypeCode e) {}
        catch (ostore.util.TypeTable.NotQuickSerializable e) { assert false; }
        receivers.put(type, cb);
    }

    protected static BigInteger rendezvous_point (
        BigInteger key, BigInteger groupname, int level) {
        BigInteger two2thel = TWO.pow (level);
        BigInteger partition_width = MOD.divide (two2thel);
	BigInteger group_offset = groupname.mod (partition_width);
        
        // use the key to find the right partition
        BigInteger partition_number = key.divide (partition_width);
        BigInteger low = partition_number.multiply (partition_width);

        return low.add (group_offset);
    }

    public static InetSocketAddress bytes2addr (byte [] bytes) {
        byte [] addrb = new byte [4];
        System.arraycopy (bytes, 0, addrb, 0, 4);
            InetAddress addr = null;
        try { addr = InetAddress.getByAddress (addrb); }
        catch (UnknownHostException e) { assert false : e; }
        ByteBuffer bb = ByteBuffer.wrap (bytes);
        bb.position (4);
        int port = bb.getShort ();
        return new InetSocketAddress (addr, port);
    }
    
    public static byte [] addr2bytes (InetSocketAddress addr) {
        byte [] result = new byte [6];
        System.arraycopy (addr.getAddress ().getAddress (), 0, result, 0, 4);
        ByteBuffer bb = ByteBuffer.wrap (result);
        bb.position (4);
        bb.putShort ((short) addr.getPort ());
        return result;
    }

    public static byte [] bi2bytes (BigInteger i) {
        byte [] result = i.toByteArray ();
        if (result.length == 20)
            return result;
        byte [] rightsize = new byte [20];
        if (result.length > 20)
            System.arraycopy (result, result.length - 20, rightsize, 0, 20);
        else 
            System.arraycopy (result, 0, rightsize, 20 - result.length,
                    result.length);
        return rightsize;
    }

    public static BigInteger bytes2bi (byte [] bytes) {
        // ensure positive
        if ((bytes [0] & 0x80) != 0) {
            byte [] copy = new byte [bytes.length + 1];
            System.arraycopy (bytes, 0, copy, 1, bytes.length);
            bytes = copy;
        }
        return new BigInteger (bytes);
    }

    protected GatewayClient client;
    protected int replication;
    protected int total_levels;
    protected long seed;
    protected boolean natted;
    protected Network network;
    protected LinkedHashMap<BigInteger,NattedMulticastClient> nattedClients =
        new LinkedHashMap<BigInteger,NattedMulticastClient>();
    protected LinkedHashMap<BigInteger,PublicMulticastClient> publicClients =
        new LinkedHashMap<BigInteger,PublicMulticastClient>();
    protected NattedMulticastParent parent;

    public void init (ConfigDataIF config) throws Exception {

	super.init (config);

        int debug_level = config_get_int (config, "debug_level");
        if (debug_level > 0) {
            logger.setLevel (Level.DEBUG);
            PublicMulticastClient.logger.setLevel(Level.DEBUG);
            NattedMulticastClient.logger.setLevel(Level.DEBUG);
            NattedMulticastParent.logger.setLevel(Level.DEBUG);
        }

        natted = config_get_boolean(config, "natted");
	
        seed  = now_ms() ^ my_node_id.hashCode();
        Random rand = new Random(seed);
        next_msg_num = rand.nextInt(Integer.MAX_VALUE);

	total_levels =  config_get_int (config, "levels");
	replication = config_get_int (config, "replication");
        String client_stg_name = 
            config_get_string (config, "client_stage_name");
        StageIF client_stg = config.getManager ().getStage (client_stg_name);
        client = (GatewayClient) client_stg.getWrapper ().getEventHandler ();

        if (! natted) {
            parent = new NattedMulticastParent(
                    acore, my_node_id.port()+1, pcall);
        }

        acore.registerTimer(0, ready);
    }

    protected Runnable ready = new Runnable() {
        public void run() {
            if (! natted) {
                network = Network.instance(my_node_id);
                try { 
                    network.registerReceiver(MulticastMessage.class, receive); 
                }
                catch (Exception e) { assert false : e; }
            }
        }
    };

    public void join(BigInteger groupname, int ttl_s, String app, 
                     ASyncCore.TimerCB cb, Object user_data) {
        if (natted) {
            NattedMulticastClient c = nattedClients.get(groupname);
            if (c == null) {
                c = new NattedMulticastClient(acore, client, seed++, 
                                              groupname, total_levels, ccall);
                nattedClients.put(groupname, c);
            }
        }
        else {
            PublicMulticastClient c = publicClients.get(groupname);
            if (c == null) {
                c = new PublicMulticastClient(my_node_id, seed++, 
                        total_levels, replication, client, acore);
                publicClients.put(groupname, c);
            }
            c.join(groupname, ttl_s, app, cb, user_data);
            parent.addGroup(groupname);
        }
    }

    public void sendMsg(BigInteger group, QuickSerializable payload) {
        if (natted)
            nattedClients.get(group).sendMsg(payload);
        else {
            publicClients.get(group).sendMsg(payload);
            MulticastMessage mm = new MulticastMessage(group, 
                    my_node_id, 0, timer_ms(), payload);
            parent.sendMsg(mm);
        }
    }

    protected Thunk2<MulticastMessage,InetSocketAddress> receive =
    new Thunk2<MulticastMessage,InetSocketAddress>() {
        public void run(MulticastMessage msg, InetSocketAddress peer) {
            // When we get a message over UDP, we must not be natted.
            assert !natted;
            // Call the locally registered receivers, 
            Thunk1<MulticastMessage> cb = receivers.get(msg.payload.getClass());
            if (cb != null) cb.run(msg);
            // forward it along the public tree,
            PublicMulticastClient c = publicClients.get(msg.group);
            if (c != null)
                c.receive(msg, peer);
            // and send it on to our natted children.
            parent.sendMsg(msg);
        }
    };

    public Thunk1<MulticastMessage> ccall = new Thunk1<MulticastMessage>() {
        public void run(MulticastMessage msg) {
            assert natted;
            // When we get a message as a client over TCP, we must be natted.
            // Call the locally registered receivers. 
            Thunk1<MulticastMessage> cb = receivers.get(msg.payload.getClass());
            if (cb != null) cb.run(msg);
        }
    };

    public Thunk1<MulticastMessage> pcall = new Thunk1<MulticastMessage>() {
        public void run(MulticastMessage msg) {
            // When we get a message as a server over TCP, we must not be
            // natted.  Call the locally registered receivers...
            assert ! natted;
            msg.sender_id = my_node_id;
            Thunk1<MulticastMessage> cb = receivers.get(msg.payload.getClass());
            if (cb != null) cb.run(msg);
            // and forward it to the rest of the public tree.
            publicClients.get(msg.group).sendMsg(msg.payload);
        }
    };
}

