package edu.boisestate.cs597.validation;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import java.awt.geom.Path2D;
import java.awt.geom.Point2D;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import edu.boisestate.cs597.util.GlobalFunctions;

/**
 *
 * @author reuben
 */

/**
public class PointTest {
    public static void main(String[] args)
    {
        StringBuilder builder = new StringBuilder();
        Map<Integer, Path2D.Double> areas = PolygonResource.extractPolygons("/home/reuben/dev/bigdata/Eucleia/data/communityareas.kml", null, false);
        HashSet<String> uncontainedPoints = new HashSet<>();
        int malformed=0;
        String line;
        Pattern comma = Pattern.compile(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        int lat = 19; int lon = 20;
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(args[0]), Charset.defaultCharset()))
        {
            while ((line = reader.readLine()) != null)
            {
                String[] split = comma.split(line);
                try
                {
                    Point2D.Double point = new Point2D.Double(GlobalFunctions.normalizeLatLon(split[lat]), GlobalFunctions.normalizeLatLon(split[lon]));
                    for (Entry<Integer, Path2D.Double> entry : areas.entrySet())
                    {
                        if (!entry.getValue().contains(point))
                        {
                            uncontainedPoints.add(line);
                        }
                    }
                }
                catch(ArrayIndexOutOfBoundsException|NumberFormatException e)
                {
                    ++malformed;
                    continue;
                }
            }
            System.out.println("Malformed lines : " + malformed);
            for (String string : uncontainedPoints)
            {
                System.out.println(string);
            }
            System.out.println("Number of uncontained points: " + uncontainedPoints.size());
        }
        catch (IOException ex)
        {
            System.err.println("Error subsampling file");
        }
    }
}
/**/