package bamboo;

import java.math.BigInteger;
import java.util.LinkedList;

import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QuickSerializable;

import bamboo.api.*;
import bamboo.util.StandardStage;

import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.EventHandlerException;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.StagesInitializedSignal;

public class SimpleStage extends StandardStage{

    protected static final long app_id = 
        bamboo.router.Router.app_id(SimpleStage.class);

    private boolean sender = false;

    protected boolean initialized = false;
    protected LinkedList wait_q = new LinkedList();

    protected static class Payload implements QuickSerializable{
        public String message;

        public Payload(String m){
            message = m;
        }

        public Payload(InputBuffer b){
            message = b.nextString();
        }

        public void serialize(OutputBuffer b){
            b.add(message);
        }

        public String toString(){
            return "Payload with message: " + message;
        }
    }

    /** Just for alarming with no further functionality */
    protected static class Alarm implements QueueElementIF{}

    /**
     * Constructor <br>
     * Calling the constructor of our super class, register 
     * all events we want to listen to.<br>
     * You should also register your own payloads here.
     */
    public SimpleStage() throws Exception {
        super(); // Sets up the logger

        // Register payloads
        ostore.util.TypeTable.register_type(Payload.class);

        // Bamboo events we wanna listen to
        event_types = new Class[] { 
                StagesInitializedSignal.class,
                BambooRouteDeliver.class,
                Alarm.class
                };
    }

    /**
     * Initialize our stage. We get the parsed Config data 
     * from sandstorm and may extract our own config options 
     * beside the global ones. <br>
     */
    public void init(ConfigDataIF config) throws Exception{
        super.init(config);

        String mode = config_get_string(config, "mode");
        if(mode != null && mode.equals("sender"))
            sender = true; // sender mode
        else
            sender = false; // default
    }

    public void handleEvent(QueueElementIF elem) {
        logger.debug("Got event " + elem);

        // If startup
        if(!initialized){
            // This one is sent when setting up
            if(elem instanceof StagesInitializedSignal){
                // Request registration for this app
                dispatch(new BambooRouterAppRegReq(
                        app_id, false, false, false, my_sink));
            }
            // OK, we are now registered to Bamboo
            else if(elem instanceof BambooRouterAppRegResp){
                // handle pending events
                initialized = true;
                while(!wait_q.isEmpty())
                    handleEvent((QueueElementIF) wait_q.removeFirst());

                // Dispatch in 10s a Alarm msg to stages on this node
		if(sender)
		    classifier.dispatch_later(new Alarm(), 10000);
            }
            // For pending events before we are registered
            else
                wait_q.addLast(elem);
        }
        // Normal opertional mode
        else{
            // Event that we got a message delivered
            if(elem instanceof BambooRouteDeliver){
                BambooRouteDeliver deliver = (BambooRouteDeliver) elem;
                Payload pay = (Payload) deliver.payload;
                logger.info("Message is: " + pay.toString());
            }
            else if(elem instanceof Alarm){
                // Create a new message to be send over bamboo, only 
                // stages with the same app_id will get this message!
                String msg = 
                    "This message should be send over" + 
                    "Bamboo to the node with the smallest node ID.";
                BambooRouteInit init = new BambooRouteInit(
                        BigInteger.ZERO, 
                        app_id, 
                        false, false, 
                        new Payload(msg));
                // Send the message to the sink.
                dispatch(init);

                // Dispatch in 10s a new Alarm msg to the stages on this node
                classifier.dispatch_later(new Alarm(), 10000);
            }
            else{
                BUG("Event " + elem + " unknown.");
            }
        }
    }

}
