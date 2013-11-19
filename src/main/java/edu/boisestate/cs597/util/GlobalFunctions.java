package edu.boisestate.cs597.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GlobalFunctions {

    //returns requests by count in descending order
    public static LinkedList<String> parseTop50(String file) throws IOException
    {
        LinkedList<String> requests = new LinkedList<>();
        Path path = new Path(file);
        FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
        InputStream is = (fs.open(new Path(path.toUri().toString())));
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, Charset.defaultCharset())))
        {
            String line;
            Pattern pattern = Pattern.compile("\\t");
            while ((line = reader.readLine()) != null)
            {
                String[] split = pattern.split(line);
                requests.addLast(split[0]);
            }
        }
        return requests;
    }

    public static void subsampleDataOnHdfs(String inputFile, String outputFile)
    {
        StringBuilder builder = new StringBuilder();
        Path path = new Path(inputFile);
        FileSystem fs = null;
        InputStream is = null;
        try
        {
            fs = FileSystem.get(path.toUri(), new Configuration());
            is = (fs.open(new Path(path.toUri().toString())));
        }
        catch (IOException e)
        {
            System.err.println("Error subsampling file");
            System.exit(1);
        }

        String line;
        int count = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, Charset.defaultCharset())))
        {
            while ((line = reader.readLine()) != null)
            {
                if (count % 10000 == 0)
                {
                    builder.append(line);
                }
                ++count;
            }
        }
        catch (IOException ex)
        {
            System.err.println("Error subsampling file");
        }
    }
    
    public static void subsampleDataToFile(String inputFile, String outputFile)
    {
        StringBuilder builder = new StringBuilder();
        String line;
        int count = 0;
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(inputFile), Charset.defaultCharset()))
        {
            while ((line = reader.readLine()) != null)
            {
                if (count % 10000 == 0)
                {
                    builder.append(line).append("\n");
                }
                ++count;
            }
            
            BufferedWriter wr = Files.newBufferedWriter(Paths.get(outputFile), Charset.defaultCharset());
            wr.append(builder.toString());
        }
        catch (IOException ex)
        {
            System.err.println("Error subsampling file");
        }
    }
}
