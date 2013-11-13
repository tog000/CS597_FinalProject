package edu.boisestate.cs597;

import java.awt.geom.Path2D;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import edu.boisestate.cs597.model.Crime;
import edu.boisestate.cs597.model.DateTypeValue;

public class MapLonLatCommunity {
	
	public static class InitialMapper extends Mapper<LongWritable, Text, Text, Crime>{

		private HashMap<Integer, Path2D.Float> communityPolygons = new HashMap<Integer, Path2D.Float>();		
		SimpleDateFormat sdf;
		@Override
		public void setup(Context context) throws IOException{
			FileSystem fs = FileSystem.get(new Configuration());
			Configuration jobConfig = context.getConfiguration();
			String kmlFile = jobConfig.get("kml_location","");
			 sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss a");
			
			if(!kmlFile.isEmpty()){
				
				Path pt=new Path(kmlFile);
				                
                try {
                	DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                	DocumentBuilder dBuilder;
					dBuilder = dbFactory.newDocumentBuilder();
					Document document = dBuilder.parse(fs.open(pt));
					
	            	document.getDocumentElement().normalize();
	            	
            		XPath xpath = XPathFactory.newInstance().newXPath();
	            	
	            	NodeList nodeList = document.getElementsByTagName("Placemark");
	            	Node communityArea, coordinates;
	            	Path2D.Float poly;
	            	
	            	for(int i=0;i<nodeList.getLength();i++){
	            		
	            		communityArea = (Node)xpath.evaluate("//name", nodeList.item(i), XPathConstants.NODE);
	            		coordinates = (Node)xpath.evaluate("//LinearRing/coordinates", nodeList.item(i), XPathConstants.NODE);
	            		
	            		poly = new Path2D.Float();
	            		
	            		String parts1[] = coordinates.getTextContent().split(" ");
	            		
	            		String parts2[];
	            		for(String coordinate : parts1){
	            			if(!coordinate.isEmpty()){
		            			parts2 = coordinate.split(",");
		            			poly.moveTo(new Float(parts2[0]), new Float(parts2[1]));
	            			}
	            		}
	            		poly.closePath();
	            		
	            		try{
	            			communityPolygons.put(Integer.valueOf(communityArea.getTextContent()), poly);
	            		}catch(Exception e){
	            			continue;
	            		}
	            		
	            	}
	            	
					
                } catch (ParserConfigurationException | SAXException | XPathExpressionException e) {
					e.printStackTrace();
				}
                
			}
			
		}
		
		@Override
		public void map(LongWritable byteOffset, Text line, Context context) throws IOException, InterruptedException{
			
			if(byteOffset.get() == 0L)return;
			
			String parts[] = line.toString().split(",",-1);
			
			if(parts.length >= 21){
				
				Crime c = new Crime();
				
				Date date;
				try{
					// Try to parse the date
					date = sdf.parse(parts[2]);
	
					Long millis = date.getTime();
					
					c.IUCR = new IntWritable(Integer.valueOf(parts[4]));
					c.locationDescription = new Text(parts[7]);
					
					c.communityArea = new IntWritable(-1);
					if(!parts[13].isEmpty()){
						c.communityArea = new IntWritable(Integer.valueOf(parts[13]));
					}
					
					c.block = new Text(parts[3]);
					c.arrest = new BooleanWritable(Boolean.parseBoolean(parts[8]));
					
					if(!parts[19].isEmpty()){
						c.lat = new FloatWritable(Float.valueOf(parts[19]));
						c.lon = new FloatWritable(Float.valueOf(parts[20]));
						
						if(c.communityArea.get() != -1){
							for(Entry<Integer, Path2D.Float> entry : communityPolygons.entrySet()){
								if(entry.getValue().contains(c.lon.get(),c.lat.get())){
									c.communityArea = new IntWritable(entry.getKey());
								}
							}
						}
					}
					
					context.write(new Text(c.communityArea.toString()), c.clone());
					
				}catch(ParseException | NumberFormatException | CloneNotSupportedException e){
					//e.printStackTrace();
					// Failed to parse, return.
					return;
				}
				
			}
		}
		
	}
	
	public static class InitialReducer extends Reducer<Text, DateTypeValue, DoubleWritable, Text>{
	
		@Override
		public void reduce(Text key, Iterable<DateTypeValue> values, Context context) throws IOException, InterruptedException{
			
			
			
			
		}
		
	}
	
	public static void main(String args[]) throws Exception{
		
		
		String test = "1948394,HH135386,01/19/2002 05:30:00 AM,066XX S DREXEL AV,0320,ROBBERY,STRONGARM - NO WEAPON,STREET,false,false,0321,,,,03,1183328,1861117,2002,03/30/2006 09:10:16 PM,41.77409866233949,-87.60350258370381,\"(41.77409866233949, -87.60350258370381)\"";
		
		String parts[] = test.split(",",-1);
		System.out.println(parts[19]);
		System.out.println(parts[20]);
		
		System.exit(1);
		
		Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(conf);
		
		GenericOptionsParser gop = new GenericOptionsParser(args);
		String[] options = gop.getRemainingArgs();
		
		Job lonLatConversionJob = new Job(new Configuration());
		lonLatConversionJob.setJarByClass(CalculateCorrelation.class);

		Configuration jobConfig = lonLatConversionJob.getConfiguration();
		jobConfig.set("kml_location",options[0]);
		
		//correlationJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		lonLatConversionJob.setMapperClass(InitialMapper.class);
        //lonLatConversionJob.setReducerClass(InitialReducer.class);
        
        lonLatConversionJob.setJobName("Find the community where the crime was commited");
        
        lonLatConversionJob.setMapOutputKeyClass(Text.class);
        lonLatConversionJob.setMapOutputValueClass(Crime.class);
		
        //lonLatConversionJob.setOutputKeyClass(DoubleWritable.class);
        //lonLatConversionJob.setOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(lonLatConversionJob, new Path(options[1]));
        Path outputPath = new Path(options[2]);
		if(fs.exists(outputPath)){
	    	fs.delete(outputPath,true);
	    }
		
        FileOutputFormat.setOutputPath(lonLatConversionJob, outputPath);
        
        System.exit(lonLatConversionJob.waitForCompletion(true) ? 1 : 0);
		
	}
	
	
}
