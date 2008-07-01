package bamboo.openhash.multicast;
import ostore.network.NetworkMessage;
import ostore.util.QuickSerializable;
import ostore.dispatch.Classifier;
import ostore.util.QSException;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.InputBuffer;

public class MulticastPingMessage extends NetworkMessage 
    implements QuickSerializable {

    public MulticastPingMessage(NodeId peer/*, Object o*/) {
	super(peer, false);
    }
    
    public MulticastPingMessage ( InputBuffer buffer ) throws QSException {
	super (buffer); 
    }
    
    
    public void serialize (OutputBuffer buffer) {
	super.serialize(buffer);
    }
    
    
    public Object clone () throws CloneNotSupportedException {
	MulticastPingMessage newone = (MulticastPingMessage) super.clone ();
	return newone;
    }
    
}
