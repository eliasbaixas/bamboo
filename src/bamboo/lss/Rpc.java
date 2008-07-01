/*
 * Copyright (c) 2001-2004 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.lss;
import bamboo.lss.Network;
import bamboo.util.Pair;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;
import ostore.util.TypeTable;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.EventHandlerIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SinkException;
import seda.sandStorm.api.SinkFullException;
import seda.sandStorm.api.SinkIF;
import static bamboo.lss.UdpCC.ByteCount;
import static bamboo.util.Curry.*;

/**
 * A simple mechanism for associating request and response messages, with
 * error checking.  To use, receivers call {@link #registerRequestHandler} to
 * set the function to be called when a request is received.  A sender sends a
 * request using {@link #sendRequest}, which will later result in a call to
 * supplied callback.  This stage will ensure that the response has the type
 * specified in the call to {@link #sendRequest}.  To send a response, the
 * receiving node calls {@link * #sendResponse} with the token passed to the
 * request callback.  
 */
public class Rpc extends bamboo.util.StandardStage {

    protected static Map<InetSocketAddress,Rpc> instances = 
        new LinkedHashMap<InetSocketAddress,Rpc>();

    protected long next_xact_id;
    protected Network network;
    protected Map<Class,Thunk3<InetSocketAddress,QuickSerializable,Object>> handlers = new LinkedHashMap<Class,Thunk3<InetSocketAddress,QuickSerializable,Object>>();
    protected Map<Long,Pair<Thunk1<QuickSerializable>,Runnable>> inflight = 
        new LinkedHashMap<Long,Pair<Thunk1<QuickSerializable>,Runnable>>();
    protected Set<Class> known_types = new LinkedHashSet<Class>();

    public static class Msg implements QuickSerializable, ByteCount {
        public boolean req;
        public long xact_id;
        public QuickSerializable payload;

        public String byteCountKey() { 
            return ((ByteCount) payload).byteCountKey(); 
        }
        public boolean recordByteCount() { 
            if (payload instanceof ByteCount) 
                return ((ByteCount) payload).recordByteCount(); 
            else 
                return false;
        }

        public Msg (boolean r, long x, QuickSerializable p) {
            req = r; xact_id = x; payload = p;
        }
        public Msg (InputBuffer buffer) throws QSException {
            req = buffer.nextBoolean ();
            xact_id = buffer.nextLong ();
            payload = buffer.nextObject ();
        }
        public void serialize (OutputBuffer buffer) {
            buffer.add (req);
            buffer.add (xact_id);
            buffer.add (payload);
        }
        public Object clone () throws CloneNotSupportedException {
            Msg result = (Msg) super.clone ();
            result.req = req;
            result.xact_id = xact_id;
            result.payload = payload;
            return result;
        }
        public String toString () {
            return "(Rpc.Msg req=" + req + " xact_id=" + xact_id + 
                " payload=" + payload + ")";
        }
    }

    protected void ensure_known (Class clazz) {
        if (! known_types.contains (clazz)) {
            known_types.add (clazz);
            try { TypeTable.register_type (clazz); } 
            catch (Exception e) { assert false : e; }
        }
    }

    protected long next_xact_id () {
        long result = next_xact_id;
        next_xact_id = (next_xact_id == Long.MAX_VALUE) ? 0 : (next_xact_id+1);
        return result;
    }

    public static Rpc instance(InetSocketAddress addr) {
        return instances.get(addr); 
    }

    public <T extends QuickSerializable> void registerRequestHandler(
            Class<T> requestType, Thunk3<InetSocketAddress,T,Object> handler) 
        throws DuplicateTypeException {
        if (handlers.containsKey(requestType))
            throw new DuplicateTypeException(requestType);
        ensure_known(requestType);
        handlers.put(requestType, 
                (Thunk3<InetSocketAddress,QuickSerializable,Object>) handler);
    }

    protected Thunk1<Long> requestTimeout = new Thunk1<Long>() {
        public void run(Long xact_id) {
            Pair<Thunk1<QuickSerializable>,Runnable> cb = 
                inflight.remove(xact_id);
            if (cb != null && cb.second != null) cb.second.run();
        }
    };

    protected static class SendToken {
        public Long xact_id; 
        public Object timerToken, networkToken;
        public SendToken(Long x, Object t, Object n) {
            xact_id = x; timerToken = t; networkToken = n;
        }
    }

    public <T extends QuickSerializable> Object sendRequest(
            InetSocketAddress dest, QuickSerializable req, long timeout_sec, 
            Class<T> resp_class, Thunk1<T> responseCallback, 
            Runnable timeoutCallback) {
        Thunk1<QuickSerializable> cb = 
            (Thunk1<QuickSerializable>) responseCallback;
        ensure_known (resp_class);
        Long xact_id = new Long (next_xact_id ());
        inflight.put (xact_id, Pair.create(
                    curry(handleResponse, cb, resp_class), 
                    timeoutCallback));
        Object timerToken = acore.registerTimer(
                timeout_sec * 1000, curry(requestTimeout, xact_id));
        Object networkToken = network.send(
                new Msg (true, xact_id.longValue (), req), dest, timeout_sec);
        return new SendToken(xact_id, timerToken, networkToken);
    }

    public void cancelSend(Object token) {
        SendToken s = (SendToken) token;
        inflight.remove(s.xact_id);
        acore.cancelTimer(s.timerToken);
        network.cancelSend(s.networkToken);
    }

    protected Thunk3<Thunk1<QuickSerializable>,Class,QuickSerializable> 
        handleResponse = 
        new Thunk3<Thunk1<QuickSerializable>,Class,QuickSerializable>() {
        public void run(Thunk1<QuickSerializable> responseCallback, 
                        Class responseClass, QuickSerializable payload) {
            if (payload == null) {
                responseCallback.run(null);
            }
            else if (responseClass.equals(payload.getClass())) {
                responseCallback.run(payload);
            }
            else if (logger.isDebugEnabled()) {
                logger.debug("bad response class; expected " + 
                        responseClass.getName() + " but got " + 
                        payload.getClass().getName());
            }
        }
    };

    public void sendResponse(QuickSerializable resp, Object token) {
        Pair<InetSocketAddress,Long> pair = 
            (Pair<InetSocketAddress,Long>) token;
        network.send(new Msg(false, pair.second.longValue(), resp),
                     pair.first);
    }

    public Rpc () {
        next_xact_id = now_ms ();
    }

    public void init(ConfigDataIF config) throws Exception {
        super.init(config);
        instances.put(my_node_id, this);
        acore.registerTimer(0, ready);
    }

    protected Runnable ready = new Runnable() {
        public void run() {
            network = Network.instance(my_node_id);
            try { network.registerReceiver(Msg.class, handleMsg); }
            catch (DuplicateTypeException e) { BUG(e); }
        }
    };

    protected Thunk2<Msg,InetSocketAddress> handleMsg = 
        new Thunk2<Msg,InetSocketAddress>() {
        public void run(Msg msg, InetSocketAddress peer) {

            if (msg.req) {
                Thunk3<InetSocketAddress,QuickSerializable,Object> cb = 
                    handlers.get(msg.payload.getClass());
                if (cb == null) {
                    if (logger.isDebugEnabled ()) 
                        logger.debug ("unknown request " 
                                      + msg.payload.getClass ());
                }
                else {
                    cb.run(peer, msg.payload, 
                            Pair.create(peer, new Long(msg.xact_id)));
                }
            }
            else {
                Pair<Thunk1<QuickSerializable>,Runnable> pair = 
                    inflight.remove(new Long(msg.xact_id));
                if (pair == null) {
                    if (logger.isDebugEnabled()) 
                        logger.debug("unknown response xact_id=" + msg.xact_id);
                }
                else {
                    pair.first.run(msg.payload);
                }
            }
        }
    };
}

