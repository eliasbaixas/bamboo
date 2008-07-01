/*
 * Copyright (c) 2001-2005 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.dht;
import bamboo.lss.ASyncCore;
import bamboo.lss.DuplicateTypeException;
import bamboo.lss.Network;
import bamboo.lss.PriorityQueue;
import bamboo.lss.Rpc;
import bamboo.router.NeighborInfo;
import bamboo.router.Router;
import bamboo.util.GuidTools;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;
import java.util.Set;
import org.apache.log4j.Logger;
import ostore.network.NetworkMessage;
import ostore.util.QSBool;
import static bamboo.dht.Dht.ForwardThroughLeafSetReq;
import static bamboo.util.Curry.*;
import static bamboo.util.StringUtil.*;

public class ReturnToClient {

    protected Logger logger;
    protected Network network;
    protected Rpc rpc;
    protected ASyncCore acore;
    protected Router router;
    protected Random rand;

    public ReturnToClient(Logger l, Network n, Rpc r, ASyncCore a, Router t, 
                          Random d) {
        logger = l; network = n; rpc = r; acore = a; router = t; rand = d;
        try {
            rpc.registerRequestHandler(
                    ForwardThroughLeafSetReq.class, fwdThruLSReq);
        }
        catch (DuplicateTypeException e) { assert false; }
    }

    public void returnToClient(final NetworkMessage msg, Thunk1<Boolean> cb) {
        // To send a message back to a client, we first just send it directly.
        long start_time = acore.timerMillis();
        if (msg instanceof Dht.GetRespMsg) {
            Dht.GetRespMsg resp = (Dht.GetRespMsg) msg;
            StringBuffer sbuf = new StringBuffer(200);
            sbuf.append("sending get resp key=0x");
            sbuf.append(GuidTools.guid_to_string(resp.key));
            sbuf.append(" return addr=");
            sbuf.append(msg.peer);
            sbuf.append(" seq=");
            sbuf.append(resp.seq);
            sbuf.append(" to ");
            sbuf.append(msg.peer);
            sbuf.append(" directly");
            logger.info(sbuf.toString());
        }
        network.send(msg, msg.peer, 5, curry(returnToClientResult, msg, 
                                             new Long(start_time), cb));
    }

    protected Thunk4<NetworkMessage,Long,Thunk1<Boolean>,Boolean> 
        returnToClientResult =
              new Thunk4<NetworkMessage,Long,Thunk1<Boolean>,Boolean>() {
        public void run(NetworkMessage msg, Long start_time, 
                        Thunk1<Boolean> cb, Boolean success) {
            if (!success.booleanValue()) {
                // If that doesn't work, we try to forward it through each
                // of the members of our leaf set.
                if (msg instanceof Dht.GetRespMsg) {
                    Dht.GetRespMsg resp = (Dht.GetRespMsg) msg;
                    StringBuffer sbuf = new StringBuffer(200);
                    sbuf.append("failed to send get resp key=0x");
                    sbuf.append(GuidTools.guid_to_string(resp.key));
                    sbuf.append(" return addr=");
                    sbuf.append(msg.peer);
                    sbuf.append(" seq=");
                    sbuf.append(resp.seq);
                    sbuf.append(" to ");
                    sbuf.append(msg.peer);
                    sbuf.append(" directly in ");
                    sbuf.append(acore.timerMillis() - start_time.longValue());
                    sbuf.append(" ms");
                    logger.info(sbuf.toString());
                }
                else if (logger.isInfoEnabled ()) {
                    StringBuffer sbuf = new StringBuffer(100);
                    sbuf.append("can't reach client ");
                    addr_to_sbuf(msg.peer, sbuf);
                    logger.info(sbuf.toString());
                }
                fwdThruLeafSet.run(msg, router.leafSet().as_set(), cb);
            }
            else {
                if (cb != null) cb.run(new Boolean(true));
                if (msg instanceof Dht.GetRespMsg) {
                    Dht.GetRespMsg resp = (Dht.GetRespMsg) msg;
                    StringBuffer sbuf = new StringBuffer(200);
                    sbuf.append("sent get resp key=0x");
                    sbuf.append(GuidTools.guid_to_string(resp.key));
                    sbuf.append(" return addr=");
                    sbuf.append(msg.peer);
                    sbuf.append(" seq=");
                    sbuf.append(resp.seq);
                    sbuf.append(" to ");
                    sbuf.append(msg.peer);
                    sbuf.append(" directly in ");
                    sbuf.append(acore.timerMillis() - start_time.longValue());
                    sbuf.append(" ms");
                    logger.info(sbuf.toString());
                }
            }
        }
    };

    protected Thunk3<NetworkMessage,Set<NeighborInfo>,Thunk1<Boolean>> 
        fwdThruLeafSet = 
        new Thunk3<NetworkMessage,Set<NeighborInfo>,Thunk1<Boolean>>() {
        public void run(NetworkMessage msg, Set<NeighborInfo> rem,
                        Thunk1<Boolean> cb) {

            // If there are any members of our leaf set that we haven't tried
            // to send the response through, pick one and give it a try.  We
            // give it a timeout of 10 seconds, and the remote node will give
            // it 5 of that to get to the client.  So 5 seconds per hop.

            // If we run out of nodes to forward it through, we just give up.
            // If the client is still alive, then its gateway will eventually
            // retry the request.

            LinkedList<NeighborInfo> ll = new LinkedList<NeighborInfo>();
            Iterator<NeighborInfo> i = rem.iterator();
            while (i.hasNext()) {
                NeighborInfo n = i.next();
                if (!router.leafSet().contains(n)) {
                    // This node is no longer in our leaf set, so we're no
                    // longer tracking whether it's up or down; don't trust it.
                    i.remove();
                }
                else if (router.possiblyDown().contains(n)) {
                    // Just skip over it for now; it may come back by the next
                    // time we try.
                }
                else {
                    ll.addLast(n);
                }
            }
            if (ll.isEmpty()) {
                // Everyone who is still in our leaf set is also in the
                // possibly down set.
                if (msg instanceof Dht.GetRespMsg) {
                    Dht.GetRespMsg resp = (Dht.GetRespMsg) msg;
                    StringBuffer sbuf = new StringBuffer(200);
                    sbuf.append("ran out of nodes to send get resp key=0x");
                    sbuf.append(GuidTools.guid_to_string(resp.key));
                    sbuf.append(" return addr=");
                    sbuf.append(msg.peer);
                    sbuf.append(" seq=");
                    sbuf.append(resp.seq);
                    sbuf.append(" through");
                    logger.info(sbuf.toString());
                }
                else if (logger.isInfoEnabled ()) {
                    StringBuffer sbuf = new StringBuffer(200);
                    sbuf.append("ran out of nodes to forward ");
                    sbuf.append(msg);
                    sbuf.append(" through");
                    logger.info(sbuf.toString());
                }
                if (cb != null) cb.run(new Boolean(false));
            }
            else {
                int which = rand.nextInt(ll.size());
                NeighborInfo neighbor = null;
                Iterator<NeighborInfo> j = ll.iterator();
                while (which-- >= 0)
                    neighbor = j.next();
                if (msg instanceof Dht.GetRespMsg) {
                    Dht.GetRespMsg resp = (Dht.GetRespMsg) msg;
                    StringBuffer sbuf = new StringBuffer(200);
                    sbuf.append("sending get resp key=0x");
                    sbuf.append(GuidTools.guid_to_string(resp.key));
                    sbuf.append(" return addr=");
                    sbuf.append(msg.peer);
                    sbuf.append(" seq=");
                    sbuf.append(resp.seq);
                    sbuf.append(" to ");
                    sbuf.append(msg.peer);
                    sbuf.append(" through ");
                    addr_to_sbuf(neighbor.node_id, sbuf);
                    logger.info(sbuf.toString());
                }
                else if (logger.isInfoEnabled ()) {
                    StringBuffer sbuf = new StringBuffer(200);
                    sbuf.append("forwarding ");
                    sbuf.append(msg);
                    sbuf.append(" to ");
                    addr_to_sbuf(msg.peer, sbuf);
                    sbuf.append(" through ");
                    addr_to_sbuf(neighbor.node_id, sbuf);
                    logger.info(sbuf.toString());
                }
                Long start_time = new Long(acore.timerMillis());
                rpc.sendRequest(neighbor.node_id, 
                                new ForwardThroughLeafSetReq(msg), 
                                10, QSBool.class, 
                                curry(fwdThruLeafSetResp, msg, rem, neighbor, 
                                      start_time, cb),
                                curry(fwdThruLeafSetTimeout, msg, rem,
                                      neighbor, start_time, cb));
            }
        }
    };

    protected Thunk6<NetworkMessage,Set<NeighborInfo>,NeighborInfo,Long,
                     Thunk1<Boolean>,QSBool> 
        fwdThruLeafSetResp = 
        new Thunk6<NetworkMessage,Set<NeighborInfo>,NeighborInfo,Long,
                         Thunk1<Boolean>,QSBool>(){
        public void run(NetworkMessage msg, Set<NeighborInfo> rem,
                        NeighborInfo peer, Long start_time, 
                        Thunk1<Boolean> cb, QSBool result) {
            router.removeFromPossiblyDown(peer);
            if (msg instanceof Dht.GetRespMsg) {
                Dht.GetRespMsg resp = (Dht.GetRespMsg) msg;
                StringBuffer sbuf = new StringBuffer(200);
                if (result.boolValue ())
                    sbuf.append("sent get resp key=0x");
                else
                    sbuf.append("failed to send get resp key=0x");
                sbuf.append(GuidTools.guid_to_string(resp.key));
                sbuf.append(" return addr=");
                sbuf.append(msg.peer);
                sbuf.append(" seq=");
                sbuf.append(resp.seq);
                sbuf.append(" to ");
                sbuf.append(msg.peer);
                sbuf.append(" through ");
                addr_to_sbuf(peer.node_id, sbuf);
                sbuf.append(" in ");
                sbuf.append(acore.timerMillis() - start_time.longValue());
                sbuf.append(" ms");
                logger.info(sbuf.toString());
            }
            if (result.boolValue ()) {
                logger.debug ("remote forward succeeded");
                if (cb != null) cb.run(new Boolean(true));
            }
            else {
                logger.debug ("remote forward failed");
                fwdThruLeafSet.run(msg, rem, cb);
            }
        }
    };

    protected Thunk5<NetworkMessage,Set<NeighborInfo>,NeighborInfo,Long,
                     Thunk1<Boolean>> 
        fwdThruLeafSetTimeout = 
        new Thunk5<NetworkMessage,Set<NeighborInfo>,NeighborInfo,Long,
               Thunk1<Boolean>>() {
        public void run(NetworkMessage msg, Set<NeighborInfo> rem, 
                        NeighborInfo peer, Long start_time,
                        Thunk1<Boolean> cb) {
            logger.debug ("forward timed out");
            router.addToPossiblyDown(peer);
            if (msg instanceof Dht.GetRespMsg) {
                Dht.GetRespMsg resp = (Dht.GetRespMsg) msg;
                StringBuffer sbuf = new StringBuffer(200);
                sbuf.append("timed out sending get resp key=0x");
                sbuf.append(GuidTools.guid_to_string(resp.key));
                sbuf.append(" return addr=");
                sbuf.append(msg.peer);
                sbuf.append(" seq=");
                sbuf.append(resp.seq);
                sbuf.append(" to ");
                sbuf.append(msg.peer);
                sbuf.append(" through ");
                addr_to_sbuf(peer.node_id, sbuf);
                sbuf.append(" in ");
                sbuf.append(acore.timerMillis() - start_time.longValue());
                sbuf.append(" ms");
                logger.info(sbuf.toString());
            }
            fwdThruLeafSet.run(msg, rem, cb);
        }
    };

    protected Thunk3<InetSocketAddress,ForwardThroughLeafSetReq,Object> fwdThruLSReq = new Thunk3<InetSocketAddress,ForwardThroughLeafSetReq,Object>() {
        public void run(InetSocketAddress src, ForwardThroughLeafSetReq req, 
                        final Object responseToken) {

            // Try to send the message to the client.

            if (req.payload instanceof Dht.GetRespMsg) {
                Dht.GetRespMsg resp = (Dht.GetRespMsg) req.payload;
                StringBuffer sbuf = new StringBuffer(200);
                sbuf.append("sending get resp key=0x");
                sbuf.append(GuidTools.guid_to_string(resp.key));
                sbuf.append(" return addr=");
                sbuf.append(req.payload.peer);
                sbuf.append(" seq=");
                sbuf.append(resp.seq);
                sbuf.append(" to ");
                sbuf.append(req.payload.peer);
                sbuf.append(" on behalf of ");
                addr_to_sbuf(src, sbuf);
                logger.info(sbuf.toString());
            }
            else {
            logger.info ("forwarding " + req.payload + " to " +
                    req.payload.peer + " on behalf of " + src);
            }
            network.send(req.payload, req.payload.peer, 5, 
                         curry(fwdThruLSResult, responseToken, src, req, 
                               new Long(acore.timerMillis())));
        }
    };

    protected Thunk5<Object,InetSocketAddress,ForwardThroughLeafSetReq,Long,Boolean> 
        fwdThruLSResult = 
          new Thunk5<Object,InetSocketAddress,ForwardThroughLeafSetReq,Long,Boolean>() {
        public void run(Object responseToken, InetSocketAddress src,
                        ForwardThroughLeafSetReq req, Long start_time, 
                        Boolean success) {
            if (success.booleanValue()) 
                logger.debug ("forward succeeded");
            else
                logger.debug ("forward failed");

            if (req.payload instanceof Dht.GetRespMsg) {
                Dht.GetRespMsg resp = (Dht.GetRespMsg) req.payload;
                StringBuffer sbuf = new StringBuffer(200);
                if (success.booleanValue()) 
                    sbuf.append("sent get resp key=0x");
                else 
                    sbuf.append("failed to send get resp key=0x");
                sbuf.append(GuidTools.guid_to_string(resp.key));
                sbuf.append(" return addr=");
                sbuf.append(req.payload.peer);
                sbuf.append(" seq=");
                sbuf.append(resp.seq);
                sbuf.append(" to ");
                sbuf.append(req.payload.peer);
                sbuf.append(" on behalf of ");
                addr_to_sbuf(src, sbuf);
                sbuf.append(" in ");
                sbuf.append(acore.timerMillis() - start_time.longValue());
                sbuf.append(" ms");
                logger.info(sbuf.toString());
            }
            // And report back whether it worked or not.
            rpc.sendResponse(new QSBool(success.booleanValue()), responseToken);
        }
    };
}

