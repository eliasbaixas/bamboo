/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.vivaldi;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Level;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.EventHandlerIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.SingleThreadedEventHandlerIF;
import seda.sandStorm.api.SinkException;
import seda.sandStorm.api.SinkFullException;
import seda.sandStorm.api.SinkClosedException;

import ostore.util.TypeTable;
import ostore.util.NodeId;
import ostore.util.SHA1Hash;
import ostore.util.ByteUtils;

import ostore.network.NetworkMessageResult;
import ostore.network.NetworkLatencyReq;
import ostore.network.NetworkLatencyResp;

import bamboo.api.BambooRouterAppRegReq;
import bamboo.api.BambooRouterAppRegResp;
import bamboo.api.BambooRouteInit;
import bamboo.api.BambooRouteDeliver;
import bamboo.router.PingMsg;

/**
 * A stage that implements the Vivaldi virtual coordinate system on top
 * of bamboo.  Vivaldi was developoed by Russ Cox, Frank Dabek,
 * Frans Kaashoek, Jinyang Li, and Robert Morris.  See their HotNets II
 * paper for details of the algorithm:
 *
 * <p>
 * Russ Cox, Frank Dabek, Frans Kaashoek, Jinyang Li, and Robert Morris.
 * "Practical, Distributed Network Coordinate".  HotNets II.  November, 2003.
 *
 * <p>
 * There are two ways we collect latency samples to update the
 * local node's virtual coordinate..  (A latency sample made up of the
 * measured latency between a remote node and that node's virtual
 * coordinate.)  You can select which sample collection method(s) you wish
 * to use through config variables.
 *
 * <p>
 * The first way is by eavesdropping on the ping messages sent by the
 * router stage.  Everytime a node is ping'ed, we also retreive its
 * virtual coordinate and use that to do updates.  The advantage of this
 * method is we get a lot of samples, but the nodes ping'ed are not
 * really random (since we only really ping nodes in the routing table).
 * This can be turned on with the <tt>eavesdrop_pings</tt> config var.
 *
 * <p>
 * The second way is to have this stage pick a random node in the overlay
 * network by routing a {@link LocateNodeMsg} message to a randomly
 * choosen guid.  Once one is found, it then pings it and retreives the
 * virtual coordinate.  This means extra network traffic is generated, but
 * the sample nodes are uniformly choosen.  A node is picked every
 * <tt>ping_period</tt> milliseconds.  This method can be turned on with
 * the <tt>generate_pings</tt> config var.
 *
 * <p>
 * For generate method, you can also have the remote node updates its
 * virtual coordinate, for no extra network traffic.  This is turned on
 * using <tt>use_reverse_ping</tt>.
 *
 * <p>
 * You may use the <tt>update_start</tt> config variable to coordinate when
 * the nodes will beginning taking samples.  You specify the time as the
 * number of milliseconds after epoch.  This is useful when you are bringing
 * up a large network of nodes.  You do not want the first few nodes to
 * converge before the rest of the network is brought up -- this leads to
 * longer overall convergence time.
 *
 * <p>
 * We also currently support three type of virtual coordinates that be used,
 * 3 dimensional, 5 dimensional, 2.5 dimensional.  The first two use
 * Euclidean distance with just different number of dimensions, and the third
 * represents points by a two dimensional coordinate and a planar distance.
 * You can specify which type you wish to use by setting <tt>vc_type</tt>
 * to one of: <tt>3d</tt>, <tt>5d</tt>,<tt>2.5d</tt>.  I suggest you use
 * the <tt>5d</tt>.
 *
 * @author Steven Czerwinski
 * @version $Id: Vivaldi.java,v 1.4 2005/08/15 21:49:06 srhea Exp $ */

public class Vivaldi extends bamboo.util.StandardStage 
  implements SingleThreadedEventHandlerIF {

      public static Vivaldi instance(InetSocketAddress addr) {
          return instances.get(addr);
      }

      public VirtualCoordinate localCoordinates() {
          return _my_coordinate;
      }

      protected static Map<InetSocketAddress,Vivaldi> instances = 
          new LinkedHashMap<InetSocketAddress,Vivaldi>();

  /**
   * Turns on/off debug messages for this stage.
   **/
  private boolean DEBUG = false;

  /**
   * This nodes virtual coordinate.
   **/
  private VirtualCoordinate _my_coordinate = null;

  /**
   * The number of ping samples that have been used to update the coordinate.
   **/
  
  private int _samples = 0;

  /**
   * The interval (seconds) between status messages.
   **/
  private long _status_period = -1;
  
  /**
   * True if we want to use reverse pings to update our virtual coordinates.
   * Reverse pings are what I call it when the "pinged" node uses the timing
   * between "LocateNodeResp" messages and "PingMsg" in order to get a rtt
   * estimate.
   **/
  private boolean _use_reverse_ping = true;

  /**
   * True if we want to eavesdrop on incoming ping messages, and use those
   * to gather samples.  This results in using whatever nodes we ping to
   * gather VC samples.. which probably is not uniform.
   **/
  private boolean _eavesdrop_pings = false;

  /**
   * True if we want to pick random nodes to ping.  In this case, this stage
   * is generating the ping messages itself.
   **/
  private boolean _generate_pings = false;
  
  /**
   * The interval (ms) of ping samples.
   **/
  private long _ping_period = 5000;

  /**
   * The time we should start updating the virtual coordinate ... or -1
   * if it should start being updated right away.
   **/
  private long _update_start = -1;

  /**
   * The initial number of samples to log.  -1 if you do not wish to
   * log any based by sample number.
   **/
  private int _track_initial = -1;
  
  /**
   * A psuedo random number generator.
   **/
  
  private Random _prng = new Random();

  /**
   * The version number for this deployment of virtual coordinates.  If we
   * get ping messages from nodes that have a different version number, then
   * we ignore them.
   **/
  private int _version_number = -1;
  
  /**
   * The identifier for vivaldi in bamboo.
   **/
  
  protected long _app_id;
  
  public Vivaldi() throws Exception {

    _app_id = ByteUtils.bytesToLong((new SHA1Hash ("bamboo.vivaldi.Vivaldi")).bytes(), new int [1]);

    try {
      ostore.util.TypeTable.register_type(bamboo.vivaldi.LocateNodeMsg.class);
      ostore.util.TypeTable.register_type(bamboo.vivaldi.LocateNodeResp.class);
      ostore.util.TypeTable.register_type(bamboo.vivaldi.PingVCResp.class);
      ostore.util.TypeTable.register_type(bamboo.vivaldi.VirtualCoordinate.class);
      ostore.util.TypeTable.register_type(bamboo.vivaldi.FiveDVC.class);
      ostore.util.TypeTable.register_type(bamboo.vivaldi.SpecialVC.class);
    } catch (Exception e) {
      logger.error("Could not register types" + e);
    }

    event_types = new Class [] {
      seda.sandStorm.api.StagesInitializedSignal.class,
      bamboo.vivaldi.VivaldiAddSample.class,
      bamboo.vivaldi.VivaldiRequestVC.class,
      bamboo.vivaldi.Vivaldi.PingAlarm.class,
      bamboo.vivaldi.Vivaldi.StatusAlarm.class };

    inb_msg_types = new Class [] {
      bamboo.vivaldi.LocateNodeResp.class,
      bamboo.router.PingMsg.class,
      bamboo.vivaldi.PingVCResp.class
    };
  }

  public void init (ConfigDataIF config) throws Exception {

    super.init (config);
    instances.put(my_node_id, this);

    DEBUG = config.getBoolean ("debug");

    String vc_type = config_get_string(config,"vc_type");
    
    if ((vc_type == null) || vc_type.equals("5d")) {
      _my_coordinate = new FiveDVC();
      logger.info("Using 5D virtual coordinates");
    } else if (vc_type.equals("3d")) {
      _my_coordinate = new VirtualCoordinate();
      logger.info("Using 3D virtual coordinates");
    } else if (vc_type.equals("2.5d")) {
      _my_coordinate = new SpecialVC();
      logger.info("Using 2.5D virtual coordinates");
    } else {
      logger.error("Invalid vc type given: "+ vc_type+ ".  Defaulting to 5D");
      _my_coordinate = new FiveDVC();
    }

    _use_reverse_ping = config_get_boolean(config,"use_reverse_ping");

    _generate_pings = config_get_boolean(config, "generate_pings");
    _eavesdrop_pings = config_get_boolean(config, "eavesdrop_pings");
    
    if (config_get_int(config,"ping_period") > 0)
      _ping_period = config.getInt("ping_period");

    if (_generate_pings && (_ping_period <= 0))
      logger.error("Generate pings set but no ping period");
    
    if (config_get_int(config,"status_period") > 0)
      _status_period = config.getInt("status_period");

    if (config_get_int(config,"seed") > 0)
      _prng = new Random(config.getInt("seed"));

    /* The time updates should start, in ms after epoch */
    _update_start = config_get_int(config, "update_start");

    _track_initial = config_get_int(config, "track_initial");

    _version_number = config_get_int(config, "version");
  }

  /**
   * Handle an event for the infrastructure node.
   **/
  
  public void handleEvent (QueueElementIF item) {

    //logger.info("Got a " + item);
    if (item instanceof seda.sandStorm.api.StagesInitializedSignal) {

      dispatch(new BambooRouterAppRegReq(_app_id, false, false, false, my_sink));
    } else if (item instanceof BambooRouterAppRegResp) { 
      BambooRouterAppRegResp resp = (BambooRouterAppRegResp) item;

      if (_generate_pings)
        scheduleNextPing(new PingAlarm());

      if (_status_period > 0)
        scheduleNextStatus(new StatusAlarm());
    } else if (item instanceof BambooRouteDeliver) {
      // something has arrived specifically for this node.. let's see what
      BambooRouteDeliver msg = (BambooRouteDeliver) item;

      if (msg.payload instanceof LocateNodeMsg) {
        handleNodeMessage((LocateNodeMsg) msg.payload);
      } else
        logger.error("got a " + msg.payload + " message");
    } else if (item instanceof LocateNodeResp) {
      handleNodeResponse((LocateNodeResp) item);
    } else if (item instanceof PingMsg) {
      if (_eavesdrop_pings) {
        PingVCResp resp = new PingVCResp((PingMsg)item,_my_coordinate,
                                         _version_number);
        resp.comp_q = null;
        dispatch(resp);
      }
    } else if (item instanceof PingVCResp) {
      PingVCResp resp = (PingVCResp) item;

      /* Skip if it's from a node with a different VC version number. */
      if (resp.version_number != _version_number)
        return;
      
      dispatch(new NetworkLatencyReq(resp.peer, my_sink, resp.coordinate));
    } else if (item instanceof NetworkLatencyResp) {
      NetworkLatencyResp resp = (NetworkLatencyResp) item;
      if (resp.success)
        handleAddSample((VirtualCoordinate)resp.user_data, resp.rtt_ms/2.0);
      
    } else if (item instanceof NetworkMessageResult) {
      /* We get this result only for the NetworkMessages we send out..
         We have two type: LocateNodeResp, and PingMsg
         we don't do anything in response to failures.
      */
      NetworkMessageResult result = (NetworkMessageResult) item;
      if (!result.success) {
        logger.warn("dropping a packet");
        return;
      }

      if (result.user_data instanceof PingCB) {
        PingCB cb = (PingCB) result.user_data;
        handleAddSample(cb.remote_coordinate, ((double)(now_ms() - cb.send_ms)/2.0));
      }
    } else if (item instanceof PingAlarm) {
      // time to take a sample
      handlePingAlarm();
      scheduleNextPing((PingAlarm)item);
    } else if (item instanceof StatusAlarm) {
      // time to print out our VC info
      handleStatusAlarm();
      scheduleNextStatus((StatusAlarm)item);
    } else if (item instanceof VivaldiAddSample) {
      VivaldiAddSample s = (VivaldiAddSample) item;
      handleAddSample(s.remote_coordinate, s.latency_ms);
    } else if (item instanceof VivaldiRequestVC) {
      VivaldiRequestVC request = (VivaldiRequestVC) item;

      VivaldiReplyVC response = new VivaldiReplyVC(_my_coordinate, _samples,
                                                   request.user_data);
      if (request.comp_q != null) { // this is just silly otherwise
        try {
          request.comp_q.enqueue(response);
        } catch (SinkClosedException e) {
          // just drop it on the ground
        } catch (SinkException e) {
          // if it's full, let's just try again in 100 ms
          classifier.dispatch_later(request, 100);
        }
      }
    } else
      logger.warn("got unexpected event: " + item + ".");
  }

  private void handleAddSample(VirtualCoordinate remote_coordinate,
                               double latency) {

    if (now_ms() <= _update_start)
      return;
    
    _my_coordinate.update(remote_coordinate, latency);
    _samples++;

    if (_samples <= _track_initial)
      logVirtualCoordinate();
  }
  
  private void handlePingAlarm() {
    /* First, pick a random guid to route to .. to find a random node. */
    BigInteger random_guid = new BigInteger(160,_prng);
    
    /* Then route a locate node message to that guid. */
    dispatch(new BambooRouteInit(random_guid, _app_id, false,false,
                                 new LocateNodeMsg(my_node_id, _my_coordinate,_version_number)));
  }

  private void handleNodeMessage(LocateNodeMsg msg) {

    /* If this message is from a node with a different VC version.. then ignore
       it. */
    if (msg.getVersion() != _version_number)
      return;
    
    LocateNodeResp reply = new LocateNodeResp(msg.getRequestor(),my_node_id,
                                              _my_coordinate);
    if (_use_reverse_ping) {
      reply.user_data = new PingCB(msg.getRequestorCoord(), now_ms());
      reply.comp_q = my_sink;
    }
    dispatch(reply);
  }
  
  private void handleNodeResponse(LocateNodeResp msg) {
    PingMsg reply = new PingMsg(msg.getLocatedId());
    reply.user_data = new PingCB(msg.getLocatedCoordinate(), now_ms());
    reply.comp_q = my_sink;
    dispatch(reply);
  }

  private void handleStatusAlarm() {
    logVirtualCoordinate();
  }

  private void logVirtualCoordinate() {
    logger.info("VC_VALUE=" + _my_coordinate + " samples=" + _samples);
  }

  private void scheduleNextPing(PingAlarm trigger) {
    /* Picks a delay of ping_period += 20%. */
    long delay = _ping_period + (long) (((_prng.nextDouble()-.5)*.4*_ping_period));
    //logger.info("Scheduling next ping for " + delay);
    classifier.dispatch_later(trigger, delay);
  }

  private void scheduleNextStatus(StatusAlarm trigger) {

    long delay = _status_period * 1000;
    delay = delay - (now_ms() % delay);
    classifier.dispatch_later(trigger,delay+5000);
    
  }

  /**
   * Used to signal that a status message should be printed.
   **/
  private static final class StatusAlarm implements QueueElementIF {
  }

  /**
   * Used to signal that a sample should be taken.
   **/
  private static final class PingAlarm implements QueueElementIF {
  }

  /**
   * Used to track the information for a sample in progress.
   **/

  private static final class PingCB {
    public long send_ms; // when the ping was sent in ms.
    public VirtualCoordinate remote_coordinate; // the vc of node we are pinging
    public PingCB(VirtualCoordinate remote, long send_time) {
      send_ms = send_time;
      remote_coordinate = remote;
    }
    
  }  
}

