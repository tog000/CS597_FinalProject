/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package validation;

import java.awt.geom.Path2D;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map.Entry;
import javax.swing.SwingUtilities;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 *
 * @author reuben
 */
public class PolygonTester {

    private static HashMap<Integer, Path2D.Double> communityPolygons = new HashMap<Integer, Path2D.Double>();
    private static SimpleDateFormat sdf;

    public static void setup(String kmlFile) throws IOException
    {
        sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss a");

        if (!kmlFile.isEmpty())
        {
            Path pt = Paths.get(kmlFile);

            try
            {
                DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder dBuilder;
                dBuilder = dbFactory.newDocumentBuilder();
                Document document = dBuilder.parse(pt.toFile());

                document.getDocumentElement().normalize();

                NodeList nodeList = document.getElementsByTagName("Placemark");
                Path2D.Double poly;

                for (int i = 0; i < nodeList.getLength(); i++)
                {
                    Node node = nodeList.item(i);

                    if (node.getNodeType() == Node.ELEMENT_NODE)
                    {
                        Element eElement = (Element) node;
                        String areaNumber = eElement.getElementsByTagName("name").item(0).getTextContent();
                        String coordinates = eElement.getElementsByTagName("coordinates").item(0).getTextContent();
                        System.out.println("community area: " + areaNumber);
                        System.out.println("First Name : " + coordinates);
                        
                        poly = new Path2D.Double();

                        String parts1[] = coordinates.split(" ");

                        String parts2[];
                        boolean moved = false;
                        for (String coordinate : parts1)
                        {
                            if (!coordinate.isEmpty())
                            {
                                parts2 = coordinate.split(",");
                                if (moved)
                                {
                                    poly.lineTo(Math.abs(new Double(parts2[0])), Math.abs(new Double(parts2[1])));
                                }
                                else
                                {
                                    poly.moveTo(Math.abs(new Double(parts2[0])), Math.abs(new Double(parts2[1])));
                                    moved = true;
                                }
                            }
                        }
                        poly.closePath();

                        try
                        {
                            communityPolygons.put(Integer.valueOf(areaNumber), poly);
                        }
                        catch (Exception e)
                        {
                            //we lose a large section of coordinates here
                            continue;
                        }

                    }

                }

            }
            catch (ParserConfigurationException | SAXException e)
            {
                e.printStackTrace();
            }

        }
    }

    public static void main(String[] args) throws IOException
    {
        setup(args[0]);
        for (final Entry<Integer, Path2D.Double> entry : communityPolygons.entrySet())
        {
            SwingUtilities.invokeLater(new Runnable() {
                @Override
                public void run()
                {
                    new PolygonFrame(entry.getKey(), entry.getValue());
                }
            });

        }
    }
}
