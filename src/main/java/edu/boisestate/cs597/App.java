package edu.boisestate.cs597;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws FileNotFoundException
    {
    	Charset charset = Charset.forName("UTF8");
    	try (BufferedReader reader = Files.newBufferedReader(FileSystems.getDefault().getPath("data", "src_2010_CSV.csv"), charset)) {
    	    String line = null;
    	    String parts[];
    	    Float lon;
    	    Float lat;
    	    while ((line = reader.readLine()) != null) {
    	    	parts = line.split(",");
    	    	if(parts[3].toLowerCase().equals("dead animal pickup")){
    	    		try{
    	    			lon = Float.valueOf(parts[20]);
    	    			lat = Float.valueOf(parts[21]);
    	    			if(lon != 0){
    	    				System.out.println(lon+","+lat);
    	    			}
    	    		}catch(Exception e){
    	    			//System.out.println("Error parsing...");
    	    		}
    	    	}
    	    }
    	} catch (IOException x) {
    	    System.err.format("IOException: %s%n", x);
    	}
        
        
        
    }
}

