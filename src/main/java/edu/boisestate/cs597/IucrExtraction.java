package edu.boisestate.cs597;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author reuben
 */
public class IucrExtraction {

    private static final Pattern csvPat = Pattern.compile(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
    private static final Pattern newLinePat = Pattern.compile("\\n");

    public static void main(String args[])
    {
        System.out.println(args);
        extractIucr(args[0], args[1], args[2]);
    }

    public static void extractIucr(String inputFile, String outputDir, String iucrCsv)
    {
        String[] iucrs = csvPat.split(iucrCsv, -1);
        final int IUCR = 4;
        final int LAT = 19;
        final int LON = 20;
        
        StringBuilder[] builders = new StringBuilder[iucrs.length];
        
        String line;
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(inputFile), Charset.defaultCharset()))
        {
        	outerloop:
            while ((line = reader.readLine()) != null)
            {
                String[] split = csvPat.split(line);
                try
                {
                    String splitIucr = split[IUCR];
                    String splitLat = split[LAT];
                    String splitLon = split[LON];
                    for (int i = 0; i < iucrs.length; i++)
                    {
                    	int numFull = 0;
                        if (splitIucr.equalsIgnoreCase(iucrs[i]))
                        {
                            StringBuilder sb = builders[i];
                            if (sb == null)
                            {
                            	sb = new StringBuilder();
                            }
                            if (newLinePat.split(sb.toString()).length >= 100)
                            {
                            	++numFull;
                            	if (numFull == iucrs.length)
                            	{
                            		break outerloop;
                            	}
                            	continue;
                            }
                            sb.append(splitLat).append(",").append(splitLon).append("\n");
                            builders[i] = sb;
                        }
                    }
                }
                catch (ArrayIndexOutOfBoundsException e)
                {
                    continue;
                }
            }

            
            for (int i = 0; i < iucrs.length; i++)
            {
                StringBuilder sb = builders[i];
                BufferedWriter wr = Files.newBufferedWriter(Paths.get(outputDir + "/" + iucrs[i] + ".txt"), Charset.defaultCharset());
                wr.append(sb.toString());
                wr.close();
            }
        }
        catch (IOException ex)
        {
            System.err.println("Error subsampling file");
        }
    }

}
