package edu.boisestate.cs597;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

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
				sdf = new SimpleDateFormat("M/d/y");
			}else{
				sdf = new SimpleDateFormat("yMd");
			}
		}
		
		@Override
		public void map(LongWritable byteOffset, Text line, Context context) throws IOException, InterruptedException{
			
			if(byteOffset.get() == 0L)return;
			
			// If we are reading the weather file
			if(isWeatherFile){
				// Split the line
				String[] parts = line.toString().split(",");
				
				try{
					// Try to parse the date
					date = sdf.parse(parts[dateColumn]);
				}catch(ParseException e){
					// Failed to parse date, return.
					return;
				}
				millis = date.getTime();
				// We want to write all these columns for every of the 50 top terms
				for(int columnNumber : relevantColumns){
					for (byte i=0;i<50;i++){
						context.write(new Text("t"+i+"w"+columnNumber), new DateTypeValue(millis,DateTypeValue.weatherPrefix+columnNumber,Float.valueOf(parts[columnNumber])));
					}
				}
				
				
			}else{ // We are reading the top50 file
				
				String[] parts = line.toString().split("\t");
				try{
					// Try to parse the date
					date = sdf.parse(parts[0]);
				}catch(ParseException e){
					// Failed to parse date, return.
					return;
				}
				millis = date.getTime();
				// Split the frequencies for given date
				frequencies = parts[1].subSequence(1, parts[1].length()-1).toString().split(",");
				
				// Write every frequency with the ranking # as the key
				for(byte currentTop50=0;currentTop50<frequencies.length;currentTop50++){
					// One for every potential weather indicator
					for(byte weatherColumn=0;weatherColumn<relevantColumns.length;weatherColumn++){
						context.write(new Text("t"+currentTop50+"w"+weatherColumn), new DateTypeValue(millis, DateTypeValue.top50Prefix, (float)currentTop50));
					}
				}
			}			
		}
	}
	
	
	public static class InitialReducer extends Reducer<LongWritable, Text, Text, IntWritable>{
		
	}

	public static void main(String[] args) throws Exception{
		
		/**/
		GenericOptionsParser gop = new GenericOptionsParser(args);
		String[] options = gop.getRemainingArgs();
		
		Job correlationJob = new Job(new Configuration());
		correlationJob.setJarByClass(CalculateCorrelation.class);
		
		// Binary FTW
		correlationJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		correlationJob.setMapperClass(InitialMapper.class);
        correlationJob.setReducerClass(InitialReducer.class);
        
        correlationJob.setJobName("Correlate Weather and daily frequencies");
        
        correlationJob.setMapOutputKeyClass(IntWritable.class);
        correlationJob.setMapOutputValueClass(DateTypeValue.class);
		
        correlationJob.setOutputKeyClass(IntWritable.class);
        correlationJob.setOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(correlationJob, new Path(options[0]));
        FileOutputFormat.setOutputPath(correlationJob, new Path(options[1]));
        
        System.exit(correlationJob.waitForCompletion(true) ? 1 : 0);
        /**/
	}

}
