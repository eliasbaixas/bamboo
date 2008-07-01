/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.router;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import ostore.util.NodeId;
import ostore.util.Pair;
import ostore.util.QuickSerializable;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.EventHandlerIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import seda.sandStorm.api.SinkException;
import seda.sandStorm.api.SinkIF;
import bamboo.api.BambooLeafSetChanged;
import bamboo.api.BambooNeighborInfo;
import bamboo.api.BambooRouteContinue;
import bamboo.api.BambooRouteDeliver;
import bamboo.api.BambooRouteInit;
import bamboo.api.BambooRouteUpcall;
import bamboo.api.BambooRouterAppRegReq;
import bamboo.api.BambooRouterAppRegResp;
import bamboo.api.BambooRoutingTableChanged;

/**
 * A wrapper around the Bamboo router which provides a function-callback style
 * interface.  Each public function of this class corresponds to the similarly
 * named Req event in bamboo.api.  Each public callback interface in this class
 * corresponds to the similarly named Resp event in bamboo.api.  See those
 * classes for details about the use of the interface.  The only real
 * difference here is that each request takes a callback and an associated
 * user_data opaque object to call it with.
 *
 * @author Sean C. Rhea
 * @version $Id: RouterCallbackInterface.java,v 1.8 2005/03/02 02:48:00 srhea Exp $
 * @deprecated <code>bamboo.router.Router</code> now directly implements all
 * of the functionality provided by this class.
 */
public class RouterCallbackInterface extends bamboo.util.StandardStage 
implements SingleThreadedEventHandlerIF {

    protected static Map instances = new HashMap ();

    public static RouterCallbackInterface instance (NodeId node_id) {
        return (RouterCallbackInterface) instances.get (node_id);
    }

    protected static class Application {
        public LeafSetChangedCB ls_cb;
        public Object ls_ud;
        public RoutingTableChangedCB rt_cb;
        public Object rt_ud;
        public RouteUpcallCB upcall_cb;
        public Object upcall_ud;
        public RouteDeliverCB deliver_cb;
        public Object deliver_ud;
        public ApplicationRegisteredCB registered_cb;
        public Object registered_ud;
    }

    protected Map apps = new HashMap ();

    public interface LeafSetChangedCB {
        void leaf_set_changed (
                BambooNeighborInfo [] preds, BambooNeighborInfo [] succs, 
                Object user_data);
    }

    public interface RoutingTableChangedCB {
        void routing_table_changed (
                BambooNeighborInfo [] added, BambooNeighborInfo [] removed, 
                Object user_data);
    }

    public interface RouteUpcallCB {
        void route_upcall (BigInteger src, BigInteger dest, 
                NodeId intermediate_src, long app_id, boolean iter,
                QuickSerializable payload, Object user_data);
    }

    public interface RouteDeliverCB {
        void route_deliver (BigInteger src, BigInteger dest, 
                NodeId immediate_src, long app_id, 
                QuickSerializable payload, Object user_data);
    }

    public interface ApplicationRegisteredCB {
        void application_registered (long app_id, 
                boolean success, String msg, BigInteger modulus, 
                int guid_digits, int digit_values, BigInteger node_guid,
                Object user_data);
    }

    public void register_app (long app_id, LeafSetChangedCB lscb, 
            Object ls_user_data, RoutingTableChangedCB rtcb, 
            Object rt_user_data, RouteUpcallCB upcb, Object up_user_data,
            RouteDeliverCB delcb, Object del_user_data,
            ApplicationRegisteredCB regcb, Object user_data) {
        Application app = new Application ();
        app.ls_cb = lscb;
        app.ls_ud = ls_user_data;
        app.rt_cb = rtcb;
        app.rt_ud = rt_user_data;
        app.upcall_cb = upcb;
        app.upcall_ud = up_user_data;
        app.deliver_cb = delcb;
        app.deliver_ud = del_user_data;
        app.registered_cb = regcb;
        app.registered_ud = user_data;
        apps.put (new Long (app_id), app);
        BambooRouterAppRegReq req = new BambooRouterAppRegReq ( app_id, 
                (lscb == null) ? false : true, 
                (rtcb == null) ? false : true, false, my_sink);
        dispatch (req);
    }

    public void route_init (BigInteger dest, long app_id, 
            boolean intermediate_upcall, boolean iter, 
            QuickSerializable payload) {
        BambooRouteInit outb = new BambooRouteInit (dest, app_id, 
                intermediate_upcall, iter, payload);
        dispatch (outb);
    }

    public void route_continue (BigInteger src, BigInteger dest, 
                NodeId immediate_src, long app_id, 
                boolean intermediate_upcall, boolean iter,
                QuickSerializable payload) {
        BambooRouteContinue outb = new BambooRouteContinue (src, dest, 
                immediate_src, app_id, intermediate_upcall, iter, payload);
        dispatch (outb);
    }

    protected void handle_app_reg_resp (BambooRouterAppRegResp resp) {
        Application app = (Application) apps.get (new Long (resp.app_id));
        app.registered_cb.application_registered (resp.app_id, resp.success, 
                resp.msg, resp.modulus, resp.guid_digits, resp.digit_values, 
                resp.node_guid, app.registered_ud);
    }

    protected void handle_leaf_set_changed (BambooLeafSetChanged msg) {
        Iterator i = apps.keySet ().iterator ();
        while (i.hasNext ()) {
            Long app_id = (Long) i.next ();
            Application app = (Application) apps.get (app_id);
            if (app.ls_cb != null) 
                app.ls_cb.leaf_set_changed (msg.preds, msg.succs, app.ls_ud);
        }
    }

    protected void handle_rt_changed (BambooRoutingTableChanged msg) {
        Iterator i = apps.keySet ().iterator ();
        while (i.hasNext ()) {
            Long app_id = (Long) i.next ();
            Application app = (Application) apps.get (app_id);
            if (app.rt_cb != null) 
                app.rt_cb.routing_table_changed (
                        msg.added, msg.removed, app.rt_ud);
        }
    }

    protected void handle_route_upcall (BambooRouteUpcall msg) {
        Application app = (Application) apps.get (new Long (msg.app_id));
        app.upcall_cb.route_upcall (msg.src, msg.dest, msg.immediate_src, 
                msg.app_id, msg.iter, msg.payload, app.upcall_ud);
    }

    protected void handle_route_deliver (BambooRouteDeliver msg) {
        Application app = (Application) apps.get (new Long (msg.app_id));
        app.deliver_cb.route_deliver (msg.src, msg.dest, msg.immediate_src, 
                msg.app_id, msg.payload, app.deliver_ud);
    }

    public RouterCallbackInterface () throws Exception {
	DEBUG = false;
    }

    public void init (ConfigDataIF config) throws Exception {
	super.init (config);
        instances.put (my_node_id, this);
    }

    public void handleEvent (QueueElementIF item) {
	if (logger.isDebugEnabled ()) logger.debug ("got " + item);

	if (item instanceof BambooRouterAppRegResp) { 
            handle_app_reg_resp ((BambooRouterAppRegResp) item);
	}
        else if (item instanceof BambooLeafSetChanged) {
            handle_leaf_set_changed ((BambooLeafSetChanged) item);
        }
        else if (item instanceof BambooRoutingTableChanged) {
            handle_rt_changed ((BambooRoutingTableChanged) item);
        }
        else if (item instanceof BambooRouteUpcall) {
            handle_route_upcall ((BambooRouteUpcall) item);
        }
        else if (item instanceof BambooRouteDeliver) {
            handle_route_deliver ((BambooRouteDeliver) item);
        }
        else {
            BUG (item.toString ());
        }
    }
}

