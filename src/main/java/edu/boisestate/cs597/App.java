package edu.boisestate.cs597;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

/**
 * @author tog
 *
 */
public class App 
{
	
	public void extractRequest(String type){
		Charset charset = Charset.forName("UTF8");
    	try (BufferedReader reader = Files.newBufferedReader(FileSystems.getDefault().getPath("data", "src_2010_CSV.csv"), charset)) {
    	    String line = null;
    	    String parts[];
    	    Float lon;
    	    Float lat;
    	    while ((line = reader.readLine()) != null) {
    	    	parts = line.split(",");
    	    	if(parts[3].toLowerCase().equals(type)){
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
	
	public void subsampleDataset(String file, int skipLines){
		Charset charset = Charset.forName("UTF8");
    	try (BufferedReader reader = Files.newBufferedReader(FileSystems.getDefault().getPath("data", file), charset)) {
    	    String line;
    	    int counter = 0;
    	    // Consume the header
    	    reader.readLine();
    	    while ((line = reader.readLine()) != null) {
    	    	
    	    	if(counter % skipLines == 0){
    	    		System.out.println(line);
    	    	}
    	    	
				counter++;
    	    }
    	} catch (IOException x) {
    	    System.err.format("IOException: %s%n", x);
    	}
	}
	
	/**
	 * 
	 * @param Crime file
	 * @param Weather File
	 * @throws ParseException 
	 */
	public void getWeatherForCrime(String file1,String file2) throws ParseException{
		Charset charset = Charset.forName("UTF8");
		SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
		
		// Create a list of dates
		LinkedList<Date> dates = new LinkedList<Date>();
    	try (BufferedReader reader = Files.newBufferedReader(FileSystems.getDefault().getPath("data", file1), charset)) {
    	    String line;
    	    // Consume the header
    	    reader.readLine();
    	    while ((line = reader.readLine()) != null) {
    	    	
    	    	Date d = sdf.parse(line.split(",")[2]);
    	    	
    	    	dates.add(d);
    	    	
    	    }
    	} catch (IOException x) {
    	    System.err.format("IOException: %s%n", x);
    	}
    	
    	sdf = new SimpleDateFormat("yyyyMMdd");
    	try (BufferedReader reader = Files.newBufferedReader(FileSystems.getDefault().getPath("data", file2), charset)) {
    	    String line;
    	    // Consume the header
    	    reader.readLine();
    	    while ((line = reader.readLine()) != null) {
    	    	
    	    	Date d = sdf.parse(line.split(",")[2]);
    	    	
    	    	if(dates.indexOf(d) != -1){
    	    		System.out.println(line);
    	    	}
    	    	
    	    }
    	} catch (IOException x) {
    	    System.err.format("IOException: %s%n", x);
    	}
    	
	}
	
	/**
	 * 
	 * @param file
	 * @param filter
	 * @throws ParseException
	 */
	public void getWeatherFromStation(String file,String filter) throws ParseException{
		Charset charset = Charset.forName("UTF8");
		
		// Create a list of dates
		LinkedList<Date> dates = new LinkedList<Date>(); 
    	try (BufferedReader reader = Files.newBufferedReader(FileSystems.getDefault().getPath("data", file), charset)) {
    	    String line;
    	    // Consume the header
    	    reader.readLine();
    	    while ((line = reader.readLine()) != null) {
    	    	
    	    	if(line.split(",")[0].equals(filter)){
    	    		System.out.println(line);
    	    	}
    	    }
    	} catch (IOException x) {
    	    System.err.format("IOException: %s%n", x);
    	}
	}
	
	public void getRequestFrequencies(String file){
		Charset charset = Charset.forName("UTF8");
		HashMap<String, Integer> hmap = new HashMap<String, Integer>(); 
    	try (BufferedReader reader = Files.newBufferedReader(FileSystems.getDefault().getPath("data", file), charset)) {
    	    String line = null;
    	    String parts[];
    	    String request;
    	    while ((line = reader.readLine()) != null) {
    	    	parts = line.split(",");
    	    	request = parts[5];//+" "+parts[6];
    	    	if(hmap.containsKey(request)){
    	    		hmap.put(request, hmap.get(request)+1);
    	    	}else{
    	    		hmap.put(request, 1);
    	    	}
    	    }
    	    
    	    ArrayList<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>(hmap.entrySet());
    		Collections.sort(list, new Comparator<Entry<String, Integer>>() {
				@Override
				public int compare(Entry<String, Integer> o1,Entry<String, Integer> o2) {
					return o2.getValue().compareTo(o1.getValue());
				}
    		});
    		
    		for(Entry<String, Integer> entry : list){
    			System.out.println(entry.getValue()+" "+entry.getKey());
    		}
    	     
    	    
    	} catch (IOException x) {
    	    System.err.format("IOException: %s%n", x);
    	}
	}
	
	
    public static void main( String[] args ) throws FileNotFoundException, ParseException
    {
        App app = new App();
        //app.extractRequest("dead animal pickup");
        //app.subsampleDataset("311nyc_2010-present.csv",1000);
        //app.getRequestFrequencies("subsampled.csv");
        //app.getWeatherForCrime("crime_chicago.csv","weather_chicago_airport.csv");
        app.getWeatherFromStation("weather_chicago.csv","GHCND:USC00111497");
    }
}

