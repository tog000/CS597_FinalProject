package edu.boisestate.cs597.validation;

import static java.awt.RenderingHints.KEY_ANTIALIASING;
import static java.awt.RenderingHints.KEY_STROKE_CONTROL;
import static java.awt.RenderingHints.VALUE_ANTIALIAS_ON;
import static java.awt.RenderingHints.VALUE_STROKE_PURE;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Container;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.geom.AffineTransform;
import java.awt.geom.Area;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Path2D;
import java.awt.geom.Point2D;

import javax.swing.JFrame;
import javax.swing.SwingUtilities;

public class PolygonFrame {

    private JFrame mainMap;
    
    public PolygonFrame(int communityArea, final Path2D.Double poly, final Point2D.Double point){
        mainMap = new JFrame();
        mainMap.setName("Community Area " + communityArea);
        mainMap.setTitle("Community Area " + communityArea);
        mainMap.setSize(500, 500);
        mainMap.setContentPane(new Container() {
            @Override
            public void paint(Graphics graphics)
            {
                Graphics2D g2 = (Graphics2D) graphics;
                //g2.setRenderingHint(KEY_ANTIALIASING, VALUE_ANTIALIAS_ON);
                //g2.setRenderingHint(KEY_STROKE_CONTROL, VALUE_STROKE_PURE);
                g2.setStroke(new BasicStroke(1f));
                g2.setColor(Color.BLUE);
                
                AffineTransform originalTransform = g2.getTransform();
                AffineTransform scaleTransform = g2.getTransform();
                scaleTransform.scale(100, 100);
                
                g2.scale(10, 10);
                g2.translate(-poly.getBounds().getCenterX()+poly.getBounds().getWidth(),-poly.getBounds().getCenterY()+poly.getBounds().getHeight());
                
                if(poly!=null){                	
                	g2.draw(poly);
                }
                
                g2.setColor(Color.RED);
                if(point!=null){
                	g2.draw(new Ellipse2D.Double(point.x-0.5, point.y-0.5, 1d, 1d));
                }
            }
        });
        mainMap.setVisible(true);
    }
    
    public PolygonFrame(int communityArea, final Path2D.Double poly){
    	this(communityArea, poly, null);
    }
    
    public PolygonFrame(Point2D.Double point, final Path2D.Double poly){
    	this(0, poly, point);
    }

    public static void main(String[] args)
    {
        Path2D.Double test = new Path2D.Double();

        //random multiples to actually show something up
//        test.moveTo(87.524620*3, 41.691801*3);
//        test.lineTo(87.525012*5, 41.691805*2);
//        test.lineTo(87.525085*4, 41.691805*4);
//        test.lineTo(87.525085*3, 41.691805*5);
//        test.lineTo(87.525279*2, 41.691806*6);
//        test.closePath();
//        final Area area = new Area(test);
//        final Path2D.Double testTwin = new Path2D.Double(test);
//        SwingUtilities.invokeLater(new Runnable() {
//            @Override
//            public void run()
//            {
//                new PolygonFrame(52, testTwin);
//            }
//        });
    }
}
