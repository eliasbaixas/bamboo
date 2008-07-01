/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.openhash.multicast;
import java.math.BigInteger;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;

public class MulticastMessage implements QuickSerializable, Cloneable {

    // The combination of (group name, the SHA of the IP:port of the first
    // public node to send this message, and the SHA of the immediate sender's
    // IP:port) is used the compute the next hops, so group and sender_id have
    // to be in here.
    //
    // The rest of the data fields (other than payload of course) are for
    // measurements taken by Harlan.  I'd like to get rid of them in some
    // clean way while still allowing for such measurements, but I haven't
    // figure out how to yet.
    // 
    // I did get rid of sequence numbers, though, as those can be easily
    // supported at the application level.

    public BigInteger group;
    public NodeId sender_id;
        
    public long est_rtt;
    public long begin_time;

    public String rtt_times;
    public String transit_times;
    public long prev_time;

    public int hops;

    public QuickSerializable payload;
    
    public MulticastMessage(BigInteger group, NodeId sender_id, long est_rtt, 
                            long begin_time, QuickSerializable payload) {
	this.group = group;
	this.sender_id = sender_id;
	this.est_rtt= est_rtt;
	this.begin_time = begin_time;
	this.rtt_times = "";
	this.transit_times = "";
	this.prev_time = begin_time;
	this.hops = 0;
        this.payload = payload;
    }
    
    public MulticastMessage ( InputBuffer buffer ) throws QSException {
	group = buffer.nextBigInteger();
	sender_id = (NodeId)buffer.nextObject();
	est_rtt = buffer.nextLong();
	begin_time = buffer.nextLong();
	rtt_times = buffer.nextString();
	transit_times = buffer.nextString();
	prev_time = buffer.nextLong();
	hops = buffer.nextInt();
	payload = buffer.nextObject();
    }
    
    public void serialize (OutputBuffer buffer) {
	buffer.add(group);
	buffer.add(sender_id);
	buffer.add(est_rtt);
	buffer.add(begin_time);
	buffer.add(rtt_times);
	buffer.add(transit_times);
	buffer.add(prev_time);
	buffer.add(hops);
	buffer.add(payload);
    }
    
    public Object clone () throws CloneNotSupportedException {
	MulticastMessage newone = (MulticastMessage) super.clone ();
	newone.group = group;
	newone.sender_id = sender_id;
	newone.est_rtt = est_rtt;
	newone.begin_time = begin_time;
	newone.rtt_times = rtt_times;
	newone.transit_times = transit_times;
	newone.prev_time = prev_time;
	newone.hops = hops;
	newone.payload = payload;
	return newone;
    }
    
    public void add_rtt(long rtt) {
	rtt_times = rtt_times + rtt + " ms; ";
    }

    public void add_transit(long transit) {
	transit_times = transit_times + (transit - prev_time) + " ms; ";
	prev_time = transit;
    }
}
