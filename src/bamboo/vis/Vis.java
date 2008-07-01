/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

package bamboo.vis;

import bamboo.lss.ASyncCore;
import bamboo.router.LeafSet;
import bamboo.router.RoutingTable;
import bamboo.util.GuidTools;
import diva.canvas.*;
import diva.canvas.connector.*;
import diva.canvas.interactor.*;
import diva.canvas.toolbox.*;
import diva.gui.BasicFrame;
import java.awt.*;
import java.awt.event.*;
import java.awt.geom.*;
import java.io.*;
import java.math.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;
import javax.swing.*;
import javax.swing.event.*;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import ostore.util.NodeId;
import static bamboo.util.Curry.*;
import static bamboo.vis.FetchNodeInfoThread.*;
import static java.nio.channels.SelectionKey.*;

/**
 * The beginnings of a Bamboo visualizer, originally cribbed from the Diva
 * Connector tutorial.
 *
 * @author Sean C. Rhea
 * @version $Id: Vis.java,v 1.49 2005/08/23 02:41:22 cgtime Exp $
 */
public class Vis {
    
    protected static Logger logger = Logger.getLogger (Vis.class);
    protected FetchNodeInfoThread FNIT;
    
    protected double ring_radius = 200.0;
    protected double dot_radius = 3.0;
    
    protected BigInteger MODULUS;
    
    protected SelectionInteractor bamboo_node_interactor;
    protected boolean ring_mode = true;
    private boolean show_color_storage = false;
    private boolean show_color_uptime = false;
        
    private JCanvas canvas;
    private GraphicsPane graphicsPane;
    
    protected JSlider zoom_slider;
    protected double zoom;
    
    protected JScrollBar x_scrollbar, y_scrollbar;
    protected double x_offset, y_offset;
    
    protected JLabel node_count_label;
    protected BasicFrame frame;
    
    protected BasicEllipse the_ring;
    
    protected long check_period_ms = 5*60*1000;
    
    private final double decimal (BigInteger guid) {
        return (new BigDecimal (guid).divide (new BigDecimal (MODULUS),
                                              6 /* scale */, BigDecimal.ROUND_UP)).doubleValue ();
    }
    
    private final double theta (BigInteger guid) {
        return 2 * Math.PI * ((decimal (guid)) + 0.75);
    }
    
    public final double x_pos_g (BigInteger guid) {
        return Math.cos (theta (guid)) * ring_radius;
    }
    
    public final double y_pos_g (BigInteger guid) {
        return Math.sin (theta (guid)) * ring_radius;
    }
    
    public class BambooNode {
        public long last_check_ms;
        public double [] coordinates;
        public BigInteger guid;
        public NodeId node_id;
        private BasicFigure dot;
        public long uptime_number;
        public String uptime_string;
        public float current_storage;
        public String ID;
        public String IP;
        public String hostname;
        public int build;
        public int estimate;
        public int port;
        public LeafSet leaf_set;
        public Map<ExtendedNeighborInfo,Long> ls_lat;
        public RoutingTable routing_table;
        public NodeInfo ninfo;
        public FetchNodeInfoThread FNIT;
        private LinkedList ls_connectors = new LinkedList ();
        private LinkedList rt_connectors = new LinkedList ();
        private LinkedList path_connectors = new LinkedList ();
        private LabelFigure guid_label;
        private boolean show_leaf_set, show_guid, show_rt;
        
        private final double decimal () {
            assert guid != null;
            return (new BigDecimal (guid).divide (new BigDecimal (MODULUS),
                                                  6, BigDecimal.ROUND_UP)).doubleValue ();
        }
        
        private final double theta () {
            return 2 * Math.PI * ((decimal ()) + 0.75);
        }
        
        public final double x_pos () {
            if (ring_mode) {
                return Math.cos (theta ()) * ring_radius - radius ();
            }
            else {
                return (coordinates == null) ? 0.0 : coordinates [0];
            }
        }
        
        public final double y_pos () {
            if (ring_mode) {
                return Math.sin (theta ()) * ring_radius - radius ();
            }
            else {
                return (coordinates == null) ? 0.0 : coordinates [1];
            }
        }
        
        public final double radius () {
            return dot_radius;
        }
        
        public final double diameter () {
            return 2.0*radius ();
        }
        
        public ArcConnector make_arc (
                                      BambooNode other, boolean exterior, Color color) {
            ConnectorTarget target = new PerimeterTarget();
            Site a = null, b = null;
            boolean succ = GuidTools.in_range_mod (guid, 
                                                   guid.add (MODULUS.divide (BigInteger.valueOf(2))), 
                                                   other.guid, MODULUS);
            if (succ) {
                a = target.getTailSite (exterior ? other.dot : dot, 0.0, 0.0);
                b = target.getHeadSite (exterior ? dot : other.dot, 0.0, 0.0);
            }
            else {
                a = target.getTailSite (exterior ? dot : other.dot, 0.0, 0.0);
                b = target.getHeadSite (exterior ? other.dot : dot, 0.0, 0.0);
            }
            ArcConnector conn = new ArcConnector (a, b);
            Rectangle2D.Double bounds = 
                (Rectangle2D.Double) conn.getBounds ();
            double len = Math.sqrt (bounds.height * bounds.height + 
                                    bounds.width * bounds.width) / 4.0;
            Arrowhead arrow = 
                new Arrowhead(b.getX(), b.getY(), b.getNormal());
            arrow.setLength (Math.min (len, dot_radius * 3));
            if (succ) {
                if (exterior)
                    conn.setTailEnd(arrow);
                else 
                    conn.setHeadEnd(arrow);
            }
            else {
                if (exterior)
                    conn.setHeadEnd(arrow);
                else
                    conn.setTailEnd(arrow);
            }
            conn.setStrokePaint (color);
            return conn;
        }
        
        public void redraw () {
            FigureLayer layer = graphicsPane.getForegroundLayer();
            if (layer.contains (dot))
                layer.remove (dot);
            dot = new BasicEllipse (x_pos (), y_pos (), diameter (), 
                                    diameter (), Color.blue);
            dot.setUserObject (this);
            dot.setInteractor (bamboo_node_interactor);
            layer.add (dot);
            redraw_leaf_set ();
            redraw_rt ();
            redraw_guid ();
        }
        
        public BambooNode (BigInteger g, NodeId n, double [] c) {
            guid = g;
            node_id = n;
            coordinates = c;
            //redraw (); Must use callbacks to do this
        }
        
        public void set_coordinates (double [] c) {
            coordinates = c;
        }
        
        public void remove () {
            hide_leaf_set ();
            hide_rt ();
            hide_guid ();
            FigureLayer layer = graphicsPane.getForegroundLayer();
            layer.remove (dot);
        }
        
        protected void undraw_leaf_set () {
            FigureLayer layer = graphicsPane.getForegroundLayer();
            while (! ls_connectors.isEmpty ()) {
                Figure conn = (Figure) ls_connectors.removeFirst ();
                layer.remove (conn);
            }
        }
        
        public void redraw_leaf_set () {
            undraw_leaf_set ();
            
            if (! show_leaf_set)
                return;
            FigureLayer layer = graphicsPane.getForegroundLayer();
            Iterator j = leaf_set.as_list ().iterator ();
            while (j.hasNext ()) {
                ExtendedNeighborInfo other_ni = (ExtendedNeighborInfo) j.next ();
                BigInteger other_g = other_ni.guid;
                BambooNode other = FNIT.nodes_by_id.get (other_g);
                if (other == null)
                    continue;
                ArcConnector conn = make_arc (other, true, Color.green);
                layer.add (conn);
                ls_connectors.add (conn);
            }
        }
        
        public void show_leaf_set () {
            show_leaf_set = true;
            redraw_leaf_set ();
        }
        
        public void hide_leaf_set () {
            show_leaf_set = false;
            undraw_leaf_set ();
        }
        
        protected void undraw_rt () {
            FigureLayer layer = graphicsPane.getForegroundLayer();
            while (! rt_connectors.isEmpty ()) {
                Figure conn = (Figure) rt_connectors.removeFirst ();
                layer.remove (conn);
            }
        }
        
        public void redraw_rt () {
            undraw_rt ();
            if (! show_rt)
                return;
            FigureLayer layer = graphicsPane.getForegroundLayer();
            ConnectorTarget target = new PerimeterTarget();
            
            Iterator j = routing_table.as_list ().iterator ();
            while (j.hasNext ()) {
                ExtendedNeighborInfo other_ni = (ExtendedNeighborInfo) j.next ();
                BigInteger other_g = other_ni.guid;
                BambooNode other = FNIT.nodes_by_id.get (other_g);
                if (other == null)
                    continue;
                Site a = target.getTailSite (dot, 0.0, 0.0);
                Site b = target.getHeadSite (other.dot, 0.0, 0.0);              
                StraightConnector conn = new StraightConnector (a, b);
                Rectangle2D.Double bounds = 
                    (Rectangle2D.Double) conn.getBounds ();
                double len = Math.sqrt (bounds.height * bounds.height + 
                                        bounds.width * bounds.width) / 4.0;
                Arrowhead arrow = 
                    new Arrowhead(b.getX(), b.getY(), b.getNormal());
                arrow.setLength (Math.min (len, dot_radius * 3));
                conn.setHeadEnd(arrow);
                conn.setStrokePaint (Color.red);
                layer.add (conn);
                rt_connectors.add (conn);
            }
            
            PathFigure pf = new PathFigure (new Line2D.Double (
                                                               0, ring_radius, 0, -1.0*ring_radius));
            pf.setDashArray (new float [] {(float) 5.0, (float) 5.0});
            layer.add (pf);
            rt_connectors.add (pf);
            for (int i = 2; i < 32; i*=2) {
                BigInteger dist = MODULUS.divide (BigInteger.valueOf (i));
                BigInteger which = guid.divide (dist);
                BigInteger dest = which.multiply (dist).add (dist.divide
                                                             (BigInteger.valueOf (2)));
                pf = new PathFigure (new Line2D.Double (0, 0, 
                                                        x_pos_g (dest), y_pos_g (dest)));
                pf.setDashArray (new float [] {(float) 5.0, (float) 5.0});
                layer.add (pf);
                rt_connectors.add (pf);
            }
        }
        
        public void show_rt () {
            show_rt = true;
            redraw_rt ();
        }
        
        public void hide_rt () {
            show_rt = false;
            undraw_rt ();
        }
        
        public int guid_anchors [] = {
            SwingConstants.SOUTH,
            SwingConstants.SOUTH_WEST,
            SwingConstants.WEST,
            SwingConstants.NORTH_WEST,
            SwingConstants.NORTH,
            SwingConstants.NORTH_EAST,
            SwingConstants.EAST,
            SwingConstants.SOUTH_EAST
        };
        
        public void undraw_guid () {
            if (guid_label != null) {
                FigureLayer layer = graphicsPane.getForegroundLayer();
                layer.remove (guid_label);
            }
            guid_label = null;
        }
        
        public void redraw_guid () {
            undraw_guid ();
            if (! show_guid)
                return;
            
            int pie = (int) ((decimal () + 1.0/16.0) * 8);
            if (pie >= guid_anchors.length)
                pie = 0;
            guid_label = new LabelFigure (
                                          "0x" + GuidTools.guid_to_string (guid) + ", " +
                                          node_id.address ().getHostName () + ":" + node_id.port () + 
                                          "\nStoring: " + current_storage + "MBs" + 
                                          "\nUptime: " + uptime_string);
            guid_label.setAnchor (guid_anchors [pie]);
            Point2D pt = CanvasUtilities.getLocation (dot.getBounds (),
                                                      CanvasUtilities.reverseDirection (guid_anchors [pie]));
            guid_label.translateTo (pt);
            FigureLayer layer = graphicsPane.getForegroundLayer();
            layer.add (guid_label);
        }
        
        public void show_path (BigInteger guid) {
            FigureLayer layer = graphicsPane.getForegroundLayer();
            BambooNode current = this;
            while (true) {
                Set ignore = new TreeSet ();
                ExtendedNeighborInfo result = null;
                if (current.leaf_set.within_leaf_set (guid)) {
                    result = new ExtendedNeighborInfo (current.leaf_set.closest_leaf (guid, ignore));
                }
                else {
                    result = new ExtendedNeighborInfo (current.routing_table.next_hop (guid, ignore));
                    if (result == null) 
                        result = new ExtendedNeighborInfo (current.leaf_set.closest_leaf (guid, ignore));
                }
                if (result.equals (new ExtendedNeighborInfo (current.node_id, current.guid)))
                    break;
                BambooNode node = FNIT.nodes_by_id.get (result.guid);
                if (node == null)
                    break;
                ArcConnector conn = current.make_arc(node, false,Color.magenta);
                layer.add (conn);
                path_connectors.add (conn);
                current = node;
            }
        }
        
        public void hide_path () {
            FigureLayer layer = graphicsPane.getForegroundLayer();
            while (! path_connectors.isEmpty ()) {
                Figure conn = (Figure) path_connectors.removeFirst ();
                layer.remove (conn);
            }
        }
        
        public void show_guid () {
            show_guid = true;
            redraw_guid ();
        }
        
        public void hide_guid () {
            show_guid = false;
            redraw_guid ();
        }
        
        public void show_color_storage () {
            float darkness = 1 - (current_storage / FetchNodeInfoThread.biggestStorageFound);
            dot.setFillPaint (new Color(darkness, darkness, darkness));
        }
        
        public void hide_color_storage () {
            color_node_default ();
        }

        public void show_color_uptime () {
            if (uptime_number == 0) {
                dot.setFillPaint (Color.RED);
            }
            else {
                float darkness = 1 - (((float) uptime_number) / ((float) FetchNodeInfoThread.biggestUptimeFound));
                dot.setFillPaint (new Color (darkness, darkness, darkness));
            }
        }

        public void hide_color_uptime () {
            color_node_default ();
        }

        public void color_node (Color C) {
            dot.setFillPaint (C);
        }

        public void color_node_default () {
            dot.setFillPaint (Color.blue);
        }
    }

    public BambooNode return_new_node (BigInteger g, NodeId n, double [] c) {
        return new BambooNode (g, n, c);
    }
    
    protected void fit_in_window () {
        
        // Find the bounding box for all points in the figure.
        
        FigureLayer layer = graphicsPane.getForegroundLayer();
        Rectangle2D allpoints = null;
        for (Iterator i = layer.figures (); i.hasNext (); ) {
            Figure f = (Figure) i.next ();
            if (allpoints == null)
                allpoints = f.getBounds ();
            else
                allpoints.add (f.getBounds());
        }
        
        // Move scrollbars to represent center of points
        
        x_scrollbar.setValue ((int)
                              Math.round (allpoints.getX () + 0.5*allpoints.getWidth ()));
        y_scrollbar.setValue ((int)
                              Math.round (allpoints.getY () + 0.5*allpoints.getHeight ()));
        
        // Adjust zoom so all points fit with a bit of space to spare
        
        Dimension2D d = (Dimension2D) canvas.getSize ();
        double xscale = d.getWidth () / allpoints.getWidth ();
        double yscale = d.getHeight () / allpoints.getHeight ();
        double desired_zoom = Math.min (xscale, yscale) * 0.8;
        zoom_slider.setValue (zoom_to_slider (desired_zoom));
    }
    
    protected void update_transform () {
        CanvasPane pane = canvas.getCanvasPane();
        Dimension2D d = (Dimension2D) canvas.getSize ();
        AffineTransform t = new AffineTransform();
        
        // Note: transforms are applied backwards.
        
        // Last, translate origin to center of frame.
        t.translate (0.5 * d.getWidth (), 0.5 * d.getHeight ());
        
        // Then, account for zoom.
        t.scale (zoom, zoom);
        
        // First, account for scrollbars.
        t.translate (x_offset, y_offset);
        
        pane.setTransform (t);
        pane.repaint ();
    }

    public Vis () {
        //This constructor is a stub for FetchNodeInfoThread.java
        //The reason it is needed is so that FNIT.java can declare new BambooNodes.
        //Since BambooNodes are an inner class of Vis.java, java requires them to
        //be associated with an actual instantiated Vis class.
    }
    
    public Vis (BigInteger mod) throws IOException{
        FNIT = new FetchNodeInfoThread (fetch_succeeded, fetch_failed, this);
        
        MODULUS = mod;
        
        JPanel xpanel = new JPanel ();
        xpanel.setOpaque (true);
        xpanel.setLayout (new BoxLayout (xpanel, BoxLayout.X_AXIS));
        
        canvas = new JCanvas ();
        graphicsPane = (GraphicsPane) canvas.getCanvasPane ();
        xpanel.add (canvas);
        
        y_offset = 0.0;
        y_scrollbar = new JScrollBar (
                                      SwingConstants.VERTICAL, 0, 100, -300, 300);
        y_scrollbar.setBlockIncrement(60);    
        y_scrollbar.setUnitIncrement(10);
        
        y_scrollbar.addAdjustmentListener (new AdjustmentListener () {
                public void adjustmentValueChanged (AdjustmentEvent e) {
                    y_offset = -1.0 * y_scrollbar.getValue ();
                    update_transform ();
                }
	    });
        xpanel.add (y_scrollbar);
        
        JPanel panel = new JPanel ();
        panel.setOpaque (true);
        panel.setLayout (new BoxLayout (panel, BoxLayout.Y_AXIS));
        panel.add (xpanel);
        
        x_offset = 0.0;
        x_scrollbar = new JScrollBar (
                                      SwingConstants.HORIZONTAL, 0, 100, -300, 300);
        x_scrollbar.setBlockIncrement(60);
        x_scrollbar.setUnitIncrement(10);
        
        x_scrollbar.addAdjustmentListener (new AdjustmentListener () {
                public void adjustmentValueChanged (AdjustmentEvent e) {
                    x_offset = -1.0 * x_scrollbar.getValue ();
                    update_transform ();
                }
	    });
        panel.add (x_scrollbar);
        
        JPanel count_and_scroll = new JPanel ();
        count_and_scroll.setOpaque (true);
        count_and_scroll.setLayout (
                                    new BoxLayout (count_and_scroll, BoxLayout.X_AXIS));
        
        count_and_scroll.add (new JLabel ("     "));
        
        node_count_label = new JLabel ("");
        update_node_count ();
        
        count_and_scroll.add (node_count_label);
        
        count_and_scroll.add (new JLabel ("     "));
        
        count_and_scroll.add (new JLabel ("Zoom:"));
        
        count_and_scroll.add (new JLabel ("     "));
        
        zoom = 1.0;
        zoom_slider = new JSlider (SwingConstants.HORIZONTAL, -100, 100, 0);
        zoom_slider.addChangeListener (new ChangeListener () {
                public void stateChanged (ChangeEvent e) {
                    zoom = slider_to_zoom (zoom_slider.getValue ());
                    update_transform ();
                }
	    });
        count_and_scroll.add (zoom_slider);
        
        count_and_scroll.add (new JLabel ("     "));
        
        panel.add (count_and_scroll);
        
        bamboo_node_interactor = new SelectionInteractor ();
        SelectionDragger dragger = new SelectionDragger (graphicsPane);
        dragger.addSelectionInteractor (bamboo_node_interactor);
        
        frame = new BasicFrame ("Bamboo Visualizer");
        frame.setSize (500, 400);
        
        frame.getContentPane ().setLayout (new BorderLayout ());
        frame.getContentPane ().add (panel);
        
        create_menus (frame);
        
        FigureLayer layer = graphicsPane.getForegroundLayer();
        if (ring_mode) {
            the_ring = new BasicEllipse(
                                        -1.0*ring_radius, -1.0*ring_radius,
                                        2.0*ring_radius, 2.0*ring_radius);
            layer.add (the_ring);
        }
        
        update_transform ();
        
        frame.setVisible (true);
    }
    
    protected double slider_to_zoom (int slider) {
        return Math.pow (10.0, zoom_slider.getValue () / 100.0);
    }
    
    protected int zoom_to_slider (double zoom) {
        // zoom = 10^(slider/100)
        // log (zoom) = slider/100
        // slider = 100*log (zoom)
        return (int) Math.round (100.0 * Math.log (zoom) / Math.log (10.0));
    }
    
    protected void create_menus (BasicFrame frame) {
        JMenuBar menu_bar = frame.getJMenuBar ();
        
        JMenu menu = menu_bar.getMenu(0);

        JMenuItem item = new JMenuItem ("Dump Leaf Set RTTs");
        menu.insert (item, 0);
        item.addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent e) {
                    dumpLeafSetRTTsDialog();
                }
	    });

        menu = new JMenu ("Nodes");
        menu.setMnemonic (KeyEvent.VK_H);
        menu_bar.add (menu);
        
        item = new JMenuItem ("Add Node");
        menu.add (item);
        item.addActionListener (new ActionListener () {
                public void actionPerformed(ActionEvent e) {
                    add_node_dialog ();
                }
	    });
        
        item = new JMenuItem ("Set Check Period");
        menu.add (item);
        item.addActionListener (new ActionListener () {
                public void actionPerformed(ActionEvent e) {
                    set_check_period_dialog ();
                }
	    });
        
        menu = new JMenu ("View");
        menu.setMnemonic (KeyEvent.VK_V);
        menu_bar.add (menu);
        
        item = new JMenuItem ("Fit In Window");
        menu.add (item);
        item.addActionListener (new ActionListener () {
                public void actionPerformed(ActionEvent e) {
                    fit_in_window ();
                }
	    });
        
        menu.addSeparator ();
        
        item = new JMenuItem ("Swap Ring Mode");
        menu.add (item);
        item.addActionListener (new ActionListener () {
                public void actionPerformed(ActionEvent e) {
                    ring_mode = ! ring_mode;                    
                    redraw_all ();
                    fit_in_window ();
                    update_transform ();
                    resize_scrollbars ();
                }
	    });
        
        menu.addSeparator ();
        
        item = new JMenuItem ("Change Dot Radius");
        menu.add (item);
        item.addActionListener (new ActionListener () {
                public void actionPerformed(ActionEvent e) {
                    change_dot_radius_dialog ();
                }
	    });
        
        menu.addSeparator ();

        item = new JMenuItem ("Find a Node");
        menu.add (item);
        item.addActionListener (new ActionListener () {
                public void actionPerformed(ActionEvent e) {
                    find_node_dialog ();
                }
	    });

        item = new JMenuItem ("Find Nodes...");
        menu.add (item);
        item.addActionListener (new ActionListener () {
                public void actionPerformed(ActionEvent e) {
                    find_nodes_dialog ();
                }
	    });        

        item = new JMenuItem ("Reset Node Colors");
        menu.add (item);
        item.addActionListener (new ActionListener () {
                public void actionPerformed(ActionEvent e) {
                    reset_node_colors ();
                }
	    });        

        menu.addSeparator ();
        
        item = new JMenuItem ("Show Path");
        menu.add (item);
        item.addActionListener (new ActionListener () {
                public void actionPerformed(ActionEvent e) {
                    show_path_dialog ();
                }
	    });
        
        item = new JMenuItem ("Hide Path");
        menu.add (item);
        item.addActionListener (new ActionListener () {
                public void actionPerformed(ActionEvent e) {
                    for_all_nodes (new ForNodeFn () {
                            public void for_node (BambooNode node) {
                                node.hide_path ();
                            }
			});
                }
	    });
        
        menu.addSeparator ();
        
        item = new JMenuItem ("Show GUIDs");
        menu.add (item);
        item.addActionListener (new ActionListener () {
                public void actionPerformed(ActionEvent e) {
                    for_all_nodes (new ForNodeFn () {
                            public void for_node (BambooNode node) {
                                node.show_guid ();
                            }
			});
                }
	    });
        
        item = new JMenuItem ("Hide GUIDs");
        menu.add (item);
        item.addActionListener (new ActionListener () {
                public void actionPerformed(ActionEvent e) {
                    for_all_nodes (new ForNodeFn () {
                            public void for_node (BambooNode node) {
                                node.hide_guid ();
                            }
			});
                }
	    });
        
        menu.addSeparator ();
        
        item = new JMenuItem ("Show leaf sets");
        menu.add (item);
        item.addActionListener (new ActionListener () {
                public void actionPerformed(ActionEvent e) {
                    for_all_nodes (new ForNodeFn () {
                            public void for_node (BambooNode node) {
                                node.show_leaf_set ();
                            }
			});
                }
	    });
        
        item = new JMenuItem ("Hide leaf sets");
        menu.add (item);
        item.addActionListener (new ActionListener () {
                public void actionPerformed(ActionEvent e) {
                    for_all_nodes (new ForNodeFn () {
                            public void for_node (BambooNode node) {
                                node.hide_leaf_set ();
                            }
			});
                }
	    });
        
        menu.addSeparator ();
        
        item = new JMenuItem ("Show routing tables");
        menu.add (item);
        item.addActionListener (new ActionListener () {
                public void actionPerformed(ActionEvent e) {
                    for_all_nodes (new ForNodeFn () {
                            public void for_node (BambooNode node) {
                                node.show_rt ();
                            }
			});
                }
	    });
        
        item = new JMenuItem ("Hide routing tables");
        menu.add (item);
        item.addActionListener (new ActionListener () {
                public void actionPerformed(ActionEvent e) {
                    for_all_nodes (new ForNodeFn () {
                            public void for_node (BambooNode node) {
                                node.hide_rt ();
                            }
			});
                }
	    });
        
        menu.addSeparator ();
        
        item = new JMenuItem ("Show storage distribution");
        menu.add (item);
        item.addActionListener (new ActionListener() {
                public void actionPerformed(ActionEvent e) {
                    Iterator i = FNIT.get_all_nodes ();
                    show_color_storage = true;
                    show_color_uptime = false;
                    while (i.hasNext ()) {
			((BambooNode) i.next ()).show_color_storage ();
                    }
                }
	    });
        
        item = new JMenuItem ("Hide storage distribution");
        menu.add (item);
        item.addActionListener (new ActionListener() {
                public void actionPerformed(ActionEvent e) {
                    Iterator i = FNIT.get_all_nodes ();
                    show_color_storage = false;
                    show_color_uptime = false;
                    while (i.hasNext ()) {
			((BambooNode) i.next ()).hide_color_storage ();
                    }
                }
	    });

        menu.addSeparator ();

        item = new JMenuItem ("Show relative uptimes");
        menu.add (item);
        item.addActionListener (new ActionListener() {
                public void actionPerformed(ActionEvent e) {
                    Iterator i = FNIT.get_all_nodes ();
                    show_color_storage = false;
                    show_color_uptime = true;
                    while (i.hasNext ()) {
			((BambooNode) i.next ()).show_color_uptime ();
                    }
                }
	    });
        
        item = new JMenuItem ("Hide relative uptimes");
        menu.add (item);
        item.addActionListener (new ActionListener() {
                public void actionPerformed(ActionEvent e) {
                    Iterator i = FNIT.get_all_nodes ();
                    show_color_storage = false;
                    show_color_uptime = false;
                    while (i.hasNext ()) {
			((BambooNode) i.next ()).hide_color_uptime ();
                    }
                }
	    });

        menu = new JMenu ("Help");
        menu.setMnemonic (KeyEvent.VK_V);
        menu_bar.add (menu);

        item = new JMenuItem ("Find a Node");
        menu.add (item);
        item.addActionListener (new ActionListener () {
                public void actionPerformed(ActionEvent e) {
                    show_help_find_node ();
                }
	    });

        item = new JMenuItem ("Find Nodes...");
        menu.add (item);
        item.addActionListener (new ActionListener () {
                public void actionPerformed(ActionEvent e) {
                    show_help_find_nodes ();
                }
	    });

        item = new JMenuItem ("Storage distribution");
        menu.add (item);
        item.addActionListener (new ActionListener () {
                public void actionPerformed(ActionEvent e) {
                    show_help_storage ();
                }
	    });
    }

    protected void show_help_find_node () {
        JOptionPane.showMessageDialog (frame,
                                       "Enter a node's ID, hostname, or IP into the input box.\n" + 
                                       "If some node matches by exact string comparison it is highlighted.",
                                       "How to use the view \"Find a Node\"",
                                       JOptionPane.INFORMATION_MESSAGE);
    }
    
    protected void show_help_find_nodes () {
        JOptionPane.showMessageDialog (frame,
                                       "Enter an expression to highlight all matching nodes.\n" +
                                       "Operands you can use are: >, >=, <, <=, =, !=, & (for joining multiple expressions)\n" +
                                       "Fields that can be used are as follows:\n" + 
                                       "build - The build of Bamboo that the node is running, a number\n" + 
                                       "hostname - The hostname of the node, a string\n" + 
                                       "ip - The IP address of the node, a string\n" + 
                                       "port - The port of the node, a number\n" + 
                                       "id - The node ID of the node, a string\n" + 
                                       "uptime - How long the node has been running, a number in seconds\n" + 
                                       "storage - How many Mbs of storage the node is holding, a floating point number in Mbs\n" + 
                                       "x_coord - The virtual x coordinate of the node, a floating point number\n" + 
                                       "y_coord - The virtual y coordinate of the node, a floating point number\n" + 
                                       "estimate - The estimated network size of this node, a number\n" + 
                                       "\n" + 
                                       "You must separate each field and operand by whitespace.\n" +
                                       "Example: \"build > 161\" matches all nodes running a build of Bamboo after 161\n" +
                                       "\n" +
                                       "You must compare numbers to numbers and strings to strings.\n" +
                                       "Example: \"build = hello\" is illegal because build is a number\n" +
                                       "Example: \"hostname > 100\" is illegal because hostname is a string and > is not allowed for strings\n" +
                                       "Example: \"hostname = 100\" IS legal, a comparison to the string 100 will be done\n" +
                                       "\n" +
                                       "Strings are a match if the shorter string is a substring of the larger.\n" +
                                       "Example: \"hostname = berkeley\" matches all nodes with berkeley somewhere in the hostname\n" +
                                       "\n" +
                                       "You cannot compare literals, you must use at least one field for each expression.\n" +
                                       "Example: \"1 > 0 & hostname = berkeley\" is illegal because the first expression uses only literals\n",
                                       "How to use the view \"Find Nodes...\"",
                                       JOptionPane.INFORMATION_MESSAGE);
    }
    
    protected void show_help_storage () {
        JOptionPane.showMessageDialog (frame,
                                       "Nodes are shaded darker according to how much storage they are holding.\n" + 
                                       "The darkest node is currently storing the most, while the lightest is\n" + 
                                       "storing the least (relative to 0 Mb).",
                                       "How to interpret the view \"Show storage distribution\"",
                                       JOptionPane.INFORMATION_MESSAGE);
    }

    protected void dumpLeafSetRTTsDialog() {
        String s = (String) JOptionPane.showInputDialog (
                                                         frame, "Dump LeafSet RTTs to File:", 
                                                         "Dump LeafSet RTTs to File", JOptionPane.PLAIN_MESSAGE);

        //If a string was returned, say so.
        if ((s != null) && (s.length() > 0)) {
            logger.info("filename: " + s);
            PrintStream os = null;
            try { os = new PrintStream(new FileOutputStream(s)); }
            catch(IOException e) { logger.warn(e); return; }
            for (BambooNode node : FNIT.nodes_by_id.values()) {
                for (ExtendedNeighborInfo ni : node.ls_lat.keySet()) {
                    os.println(node.node_id + " " + ni.node_id + " " 
                               + node.ls_lat.get(ni));
                }
            }
            os.close();
        }
    }
    
    protected void add_node_dialog () {
        String s = (String) JOptionPane.showInputDialog (
                                                         frame, "Add node:", "Add Node",
                                                         JOptionPane.PLAIN_MESSAGE);
        
        //If a string was returned, say so.
        if ((s != null) && (s.length() > 0) && (FNIT != null)) {
            FNIT.add_work (s, null);
        }
    }
    
    protected void show_path_dialog () {
        String s = (String) JOptionPane.showInputDialog (
                                                         frame, "Enter identifier to look up (in hex)", 
                                                         "Change Dot Radius",
                                                         JOptionPane.PLAIN_MESSAGE,
                                                         null, null, "0x12345678");
        
        if (s != null) {
            if (s.indexOf ("0x") == 0)
                s = s.substring (2);
            BigInteger blah = new BigInteger (s, 16);
            blah = blah.multiply (BigInteger.valueOf (2).pow (128));
            final BigInteger dest = blah;
            for_all_nodes (new ForNodeFn () {
                    public void for_node (BambooNode node) {
			node.show_path (dest);
                    }
                });
        }
    }
    
    protected void change_dot_radius_dialog () {
        String s = (String) JOptionPane.showInputDialog (
                                                         frame, "Change dot radius:", "Change Dot Radius",
                                                         JOptionPane.PLAIN_MESSAGE,
                                                         null, null, Double.toString (dot_radius));
        
        if (s != null) {
	    double value = Double.parseDouble (s);
	    set_dot_radius (value);
        }
    }

    protected void find_node_dialog () {
        String s = (String) JOptionPane.showInputDialog (
                                                         frame, "Enter a node's ID, hostname, or IP:", "Find a Node",
                                                         JOptionPane.PLAIN_MESSAGE,
                                                         null, null, null);
        show_color_storage = false;
        show_color_uptime = false;

        BambooNode matching_node = FNIT.find_node (s);

        reset_node_colors ();

        if (matching_node != null) {
            matching_node.color_node (Color.RED);
            matching_node.show_guid ();
        }
    }

    protected void reset_node_colors () {
        BambooNode current_node;
        Iterator IT = FNIT.get_all_nodes ();

        while (IT.hasNext ()) {
            current_node = (BambooNode) IT.next ();
            current_node.color_node_default ();
            current_node.hide_guid ();
        }
    }

    protected void find_nodes_dialog () {
        String s = (String) JOptionPane.showInputDialog (
                                                         frame,
                                                         "Operands: >, >=, <, <=, =, !=, &\n" +
                                                         "Fields: build, ip, port, id, uptime, storage, x_coord, y_coord, estimate",
                                                         "Find Nodes...",
                                                         JOptionPane.PLAIN_MESSAGE,
                                                         null, null, null);
        show_color_storage = false;
        show_color_uptime = false;

        LinkedList matching_nodes = FNIT.find_nodes (s);
        if (FetchNodeInfoThread.test_bad_find_nodes_input (matching_nodes)) {
            JOptionPane.showMessageDialog (frame, "Incorrect input");
        }
        else {
            reset_node_colors ();

            if (matching_nodes.size () == 0) {
                JOptionPane.showMessageDialog (frame, "No matching nodes");
            }
            else {
                Iterator i = matching_nodes.iterator ();
                while (i.hasNext ()) {
                    ((BambooNode) i.next ()).color_node (Color.RED);
                }
            }
        }
    }

    protected void set_check_period_dialog () {
        String s = (String) JOptionPane.showInputDialog (
                                                         frame, "Set check period (seconds):", "Set Check Period",
                                                         JOptionPane.PLAIN_MESSAGE,
                                                         null, null, Long.toString (check_period_ms / 1000));
        if (s != null) {
            check_period_ms = Long.parseLong (s) * 1000;
            if (FNIT != null) {
                FNIT.check_period_ms = check_period_ms;
            }
        }
    }
    
    protected void update_node_count () {
        node_count_label.setText ("nodes up: " + FNIT.nodes_by_id.size ());
        node_count_label.repaint ();
    }
    
    protected void set_dot_radius (double value) {
        dot_radius = value;
        redraw_all ();
    }
    
    protected void redraw_all () {
        FigureLayer layer = graphicsPane.getForegroundLayer();
        if ((! ring_mode) && (the_ring != null)) {
            layer.remove (the_ring);
            the_ring = null;
        }
        if (ring_mode && (the_ring == null)) {
            the_ring = new BasicEllipse(
                                        -1.0*ring_radius, -1.0*ring_radius,
                                        2.0*ring_radius, 2.0*ring_radius);
            the_ring.setLineWidth ((float) (dot_radius/3.0));
            the_ring.repaint ();
            layer.add (the_ring);
        }
        for (BambooNode b : FNIT.nodes_by_id.values ())
	    b.redraw ();
    }
    
    protected static interface ForNodeFn {
        void for_node (BambooNode node);
    }
    
    public void for_all_nodes (ForNodeFn fn) {
        FigureLayer layer = graphicsPane.getForegroundLayer();
        SelectionModel model = bamboo_node_interactor.getSelectionModel();
        Iterator i = model.getSelection ();
        while (i.hasNext ()) {
            Figure dot = (Figure) i.next ();
            if (dot.getUserObject () instanceof BambooNode) {
                BambooNode node = (BambooNode) dot.getUserObject ();
                fn.for_node (node);
            }
        }
    }
    
    public void resize_scrollbars () {        
        FigureLayer layer = graphicsPane.getForegroundLayer();
        Rectangle2D allpoints = null;
        for (Iterator i = layer.figures (); i.hasNext (); ) {
            Figure f = (Figure) i.next ();
            if (allpoints == null)
                allpoints = f.getBounds ();
            else
                allpoints.add (f.getBounds());
        }

        double max_x_coor = allpoints.getX () + allpoints.getWidth ();
        double min_x_coor = allpoints.getX ();
        double max_y_coor = allpoints.getY () + allpoints.getHeight ();
        double min_y_coor = allpoints.getY ();
        
        y_scrollbar.setValues((int) Math.round (allpoints.getY () + 0.5*allpoints.getHeight ()),
                              (int) Math.round (0.25 * allpoints.getHeight ()),
                              (int) Math.round ((min_y_coor) - Math.abs (min_y_coor * 0.01)),
                              (int) Math.round ((max_y_coor) + Math.abs (max_y_coor * 0.01)));

        x_scrollbar.setValues((int) Math.round (allpoints.getX () + 0.5*allpoints.getWidth ()),
                              (int) Math.round (0.25 * allpoints.getWidth ()),
                              (int) Math.round ((min_x_coor) - Math.abs (min_x_coor * 0.01)),
                              (int) Math.round ((max_x_coor) + Math.abs (max_x_coor * 0.01)));
        
        int x_block_increment = (int) Math.round ((max_x_coor - min_x_coor) / 10);
        x_scrollbar.setBlockIncrement ((x_block_increment > 10) ? x_block_increment : 10);
        x_scrollbar.setUnitIncrement ((x_block_increment > 40) ? (x_block_increment / 40) : 1);
        
        int y_block_increment = (int) Math.round ((max_y_coor - min_y_coor) / 10);
        y_scrollbar.setBlockIncrement ((y_block_increment > 10) ? y_block_increment : 10);
        y_scrollbar.setUnitIncrement ((y_block_increment > 40) ? (y_block_increment / 40) : 1);

        Dimension2D d = (Dimension2D) canvas.getSize ();
        double xscale = d.getWidth () / allpoints.getWidth ();
        double yscale = d.getHeight () / allpoints.getHeight ();
        double desired_zoom = Math.min (xscale, yscale) * 0.8;
        zoom_slider.setValue (zoom_to_slider (desired_zoom));
    }
    
    protected Thunk1<BambooNode> fetch_succeeded = new Thunk1<BambooNode> () {
        public void run (BambooNode node) {
            node.redraw ();
            update_node_count ();
            node.redraw_leaf_set ();
            //Re-color nodes if needed to reflect storage distribution
            if (show_color_storage) {
                Iterator IT = FNIT.get_all_nodes ();
                while (IT.hasNext ()) {
                    ((BambooNode) IT.next ()).show_color_storage ();
                }
            }
            else if (show_color_uptime) {
                Iterator IT = FNIT.get_all_nodes ();
                while (IT.hasNext ()) {
                    ((BambooNode) IT.next ()).show_color_uptime ();
                }
            }
            
            resize_scrollbars ();
        }
    };
    
    protected Thunk1<BambooNode> fetch_failed = new Thunk1<BambooNode> () {
        public void run (BambooNode node) {
            if (node != null) {
                node.remove ();
                update_node_count ();
            }
        }
    };
        
    public void run (String [] args) throws IOException{
        fit_in_window ();
        redraw_all ();        

        FNIT.run ();
    }
    
    public static void main (String [] args) throws IOException {
        PatternLayout pl = new PatternLayout ("%d{ISO8601} %-5p %c: %m\n");
        ConsoleAppender ca = new ConsoleAppender (pl);
        
        Logger.getRoot ().addAppender (ca);
        Logger.getRoot ().setLevel (Level.INFO);

        Vis vis = new Vis (BigInteger.valueOf (2).pow (160));
        vis.run (args);
    }
}
