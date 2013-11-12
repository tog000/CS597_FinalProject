package edu.boisestate.cs597;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.stat.correlation.SpearmansCorrelation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.boisestate.cs597.model.DateTypeValue;

public class CalculateCorrelation {
	
	//TODO 
	// 2013-11-06 We need weather for every day and frequencies for every of the top50 for every day
	// group by top50 id and emit 50*n different weather variables n
	// reducer receives as key the top50 ID and values will be 500 days of n different weather indicators, and 500 days of frequencies
	//
	// 2013-11-07 Essentially what we are doing is a complete join, all the rows from one file against all the rows from the other!
	
	public static class InitialMapper extends Mapper<LongWritable, Text, Text, DateTypeValue>{
		
		private boolean isWeatherFile = false;
		
		//
		// For REQUESTS files
		//
		private SimpleDateFormat sdf;
		private Date date;
		private long millis;
		private String frequencies[];
				
		//
		// For WEATHER files
		//
		private static int dateColumn = 1;
		/**
		 * 2 = Precipitation (tenths of mm) (PRCP)
		 * 3 = Snow depth (mm) (SNWD)
		 * 4 = Snowfall (mm) (SNOW)
		 * 5 = Maximum temperature (tenths of degrees C) (TMAX)
		 * 6 = Minimum temperature (tenths of degrees C) (TMIN)
		 * 7 = Average daily wind speed (tenths of meters per second) (AWND)
		 * 
		 */
		private static int[] relevantColumns = {2,3,4,5,6,7};
		
		@Override
		public void setup(Context context){
			
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().toString();
			
			if(filename.contains("weather")){
				isWeatherFile = true;
				sdf = new SimpleDateFormat("yyyyMMdd");
			}else{
				sdf = new SimpleDateFormat("MM/dd/yyyy");
			}
		}
		
		@Override
		public void map(LongWritable byteOffset, Text line, Context context) throws IOException, InterruptedException{
			
			// If we are reading the weather file
			if(isWeatherFile){
				
				if(byteOffset.get() == 0L)return;
				
				// Split the line
				String[] parts = line.toString().split(",");
				
				try{
					// Try to parse the date
					date = sdf.parse(parts[dateColumn]);
				}catch(ParseException e){
					//e.printStackTrace();
					// Failed to parse date, return.
					return;
				}
				millis = date.getTime();
				System.out.println("RelevantColumns="+Arrays.toString(relevantColumns));
				// We want to write all these columns for every of the 50 top terms
				for(int columnNumber : relevantColumns){
					for (byte i=0;i<50;i++){
						//System.out.printf("%s -> (%d,%s,%d)","t"+i+"w"+columnNumber,millis, DateTypeValue.weatherPrefix+columnNumber,i);
						context.write(new Text("t"+i+"w"+columnNumber), new DateTypeValue(millis,DateTypeValue.weatherPrefix+columnNumber,Float.valueOf(parts[columnNumber])));
					}
				}
				
				
			}else{ // We are reading the top50 file
				
				String[] parts = line.toString().split("\t");
				try{
					// Try to parse the date
					date = sdf.parse(parts[0]);
				}catch(ParseException e){
					//e.printStackTrace();
					// Failed to parse date, return.
					return;
				}
				millis = date.getTime();
				// Split the frequencies for given date
				frequencies = parts[1].subSequence(1, parts[1].length()-1).toString().split(",");
				
				System.out.println("RelevantColumns="+Arrays.toString(frequencies));
				// Write every frequency with the ranking # as the key
				for(byte currentTop50=0;currentTop50<frequencies.length;currentTop50++){
					// One for every potential weather indicator
					for(int weatherColumn : relevantColumns){
						//System.out.printf("%s -> (%d,%s,%d)","t"+currentTop50+"w"+weatherColumn,millis, DateTypeValue.top50Prefix, currentTop50);
						context.write(new Text("t"+currentTop50+"w"+weatherColumn), new DateTypeValue(millis, DateTypeValue.top50Prefix, Float.valueOf(frequencies[currentTop50])));
					}
				}
			}			
		}
	}
	
	private static class Point{
		public Float x = null;
		public Float y = null;
		public boolean isComplete(){return x!=null && y!=null;}
		@Override
		public String toString(){return x.toString()+","+y.toString();}
	}
	
	public static class InitialReducer extends Reducer<Text, DateTypeValue, DoubleWritable, Text>{
		
		HashMap<Long, Point> dateMap = new HashMap<Long, Point>();
		
		
		// We must align the dates
		@Override
		public void reduce(Text key, Iterable<DateTypeValue> values, Context context) throws IOException, InterruptedException{
			
			for(DateTypeValue dtv : values){
				
				if(!dateMap.containsKey(dtv.date.get())){
					dateMap.put(dtv.date.get(),new Point());
				}
				
				if(dtv.isWeather()){
					dateMap.get(dtv.date.get()).y = dtv.value.get();
				}else{
					dateMap.get(dtv.date.get()).x = dtv.value.get();
				}
			}
			
			Collection<Point> pointValues = dateMap.values();
			Collection<Point> goodPoints = new LinkedList<Point>();;
			
			LinkedList<Float> xArray = new LinkedList<Float>();
			LinkedList<Float> yArray = new LinkedList<Float>();
			
			String buffer = "";
			
			for(Point p : pointValues){
				if(p.isComplete()){
					goodPoints.add(p);
					xArray.add(p.x);
					yArray.add(p.y);
					buffer += p.toString()+"|";
				}
			}
			
			double xPrimitive[] = new double[xArray.size()];
			double yPrimitive[] = new double[yArray.size()];
			
			int i=0;
			for(Float v:xArray) xPrimitive[i++] = v;
			i=0;
			for(Float v:yArray) yPrimitive[i++] = v;
			
			SpearmansCorrelation sc = new SpearmansCorrelation();
			double rho = sc.correlation(xPrimitive, yPrimitive);
			
			/*// Find correlation 
			// TODO use apache commons to find means
			for(int i=0;i<xs.size();i++){
				totalX += xs.get(i);
				totalY += ys.get(i);
			}
			
			float avgX = totalX/((float)xs.size());
			float avgY = totalY/((float)ys.size());
			
			float term1 = 0.0f;
			float term2 = 0.0f;
			float term3 = 0.0f;
			
			for(Point p : goodPoints){
				term1 += (p.x - avgX)*(p.y - avgY);
				term2 += Math.pow(p.x - avgX,2);
				term3 += Math.pow(p.y - avgY,2);
			}
			
			Double rho = term1/(Math.sqrt(term2*term3));*/
			
			//System.out.printf("For %s, freq=%d, weather=%d\n",key, totalFrequencies,totalWeatherPoints);
			//System.out.printf("For %s, good points=%d\n",key, totalGoodPoints);
			context.write(new DoubleWritable(Math.abs(rho)), new Text(key.toString()+"\t"+rho+"\t"+buffer));
			
		}
		
	}

	public static void main(String[] args) throws Exception{
		
		/**

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Date d = sdf.parse("20130101");
		
		SimpleDateFormat sdf2 = new SimpleDateFormat("MM/dd/yyyy");
		Date d2 = sdf2.parse("01/01/2013");
		
		System.out.println(d.getTime());
		System.out.println(d2.getTime());
		
		/*/
		
		Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(conf);
		
		GenericOptionsParser gop = new GenericOptionsParser(args);
		String[] options = gop.getRemainingArgs();
		
		Job correlationJob = new Job(new Configuration());
		correlationJob.setJarByClass(CalculateCorrelation.class);
		
		// Binary FTW
		//correlationJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		correlationJob.setMapperClass(InitialMapper.class);
        correlationJob.setReducerClass(InitialReducer.class);
        
        correlationJob.setJobName("Correlate Weather and daily frequencies");
        
        correlationJob.setMapOutputKeyClass(Text.class);
        correlationJob.setMapOutputValueClass(DateTypeValue.class);
		
        correlationJob.setOutputKeyClass(DoubleWritable.class);
        correlationJob.setOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(correlationJob, new Path(options[0]));
        Path outputPath = new Path(options[1]);
		if(fs.exists(outputPath)){
	    	fs.delete(outputPath,true);
	    }
		
        FileOutputFormat.setOutputPath(correlationJob, new Path(options[1]));
        
        System.exit(correlationJob.waitForCompletion(true) ? 1 : 0);
        /**/
	}

}
