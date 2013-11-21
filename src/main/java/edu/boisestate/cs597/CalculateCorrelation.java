package edu.boisestate.cs597;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;

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

import edu.boisestate.cs597.model.Crime;
import edu.boisestate.cs597.model.DateTypeValue;
import edu.boisestate.cs597.util.GlobalFunctions;

public class CalculateCorrelation {
	
	public static class InitialMapper extends Mapper<LongWritable, Text, Text, DateTypeValue>{
		
		private enum FILE_TYPE {WEATHER_FILE, HEALTH_FILE, ECONOMIC_FILE, CRIME_FILE};
		private FILE_TYPE fileType;
		
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

		private static int[] relevantWeatherColumns = {2,3,4,5,6,7};
		private static String[] relevantWeatherColumnNames = {"PRCP","SNWD","SNOW","TMAX","TMIN","AWND"};
		
		/*
		
		0 STATION
		1 STATION_NAME
		2 DATE
		3 MXPN
		4 MNPN
		5 EVAP
		6 MDEV
		7 DAEV
		8 SX32
		9 SX52
		10 SX53
		11 SN32
		12 SN52
		13 SN53
		14 MDPR
		15 MDSF
		16 DAPR
		17 DASF
		18 PRCP
		19 SNWD
		20 SNOW
		21 TSUN
		22 TMAX
		23 TMIN
		24 TOBS
		25 WESD
		26 WESF
		27 AWND
		
		SN32 - Minimum soil temperature (tenths of degrees C) with bare ground cover at 10 cm depth
		DAPR - Number of days included in the multiday precipitation total (MDPR)
		AWND - Average daily wind speed (tenths of meters per second)
		DASF - Number of days included in the multiday snow fall total (MDSF) 
		SNOW - Snowfall (mm)
		MXPN - Daily maximum temperature of water in an evaporation pan (tenths of degrees C)
		TMIN - Minimum temperature (tenths of degrees C)
		WESF - Water equivalent of snowfall (tenths of mm)
		WESD - Water equivalent of snow on the ground (tenths of mm)
		TMAX - Maximum temperature (tenths of degrees C)
		MDEV - Multiday evaporation total (tenths of mm; use with DAEV)
		SNWD - Snow depth (mm)
		MNPN - Daily minimum temperature of water in an evaporation pan (tenths of degrees C)
		MDPR - Multiday precipitation total (tenths of mm; use with DAPR and DWPR, if available)
		EVAP - Evaporation of water from evaporation pan (tenths of mm)
		PRCP - Precipitation (tenths of mm)
		DAEV - Number of days included in the multiday evaporation total (MDEV)
		SX52 - Maximum soil temperature (tenths of degrees C) with sod cover at 10 cm depth
		SN53 - Minimum soil temperature (tenths of degrees C) with sod cover at 20 cm depth
		TSUN - Daily total sunshine (minutes)
		SX53 - Maximum soil temperature (tenths of degrees C) with sod cover at 20 cm depth
		SN52 - Minimum soil temperature (tenths of degrees C) with sod cover at 10 cm depth
		SX32 - Maximum soil temperature (tenths of degrees C) with bare ground cover at 10 cm depth
		TOBS - Temperature at the time of observation (tenths of degrees C)
		MDSF - Multiday snowfall total
		*/
		
		//
		// For HEALTH files
		//
		
		private static int healthCommunityAreaColumn = 0;
		private static int[] relevantHealthColumns = {4,5,6,7,10,18,26,27, 28};
		private static String[] relevantHealthColumnNames = {"LOW_BIRTH_WEIGHT","PRENATAL_CARE","PRETERM_BIRTH","TEEN_BIRTH","CANCER","BLOOD_LEAD","NO_HIGHSCHOOL","INCOME","UNEMPLOYMENT"};
		
		/*
		0  Community Area
		1  Community Area Name
		2  Birth Rate
		3  General Fertility Rate
		4  Low Birth Weight
		5  Prenatal Care Beginning in First Trimester
		6  Preterm Births
		7  Teen Birth Rate
		8  Assault (Homicide)
		9  Breast cancer in females
		10  Cancer (All Sites)
		11  Colorectal Cancer
		12  Diabetes-related
		13  Firearm-related
		14  Infant Mortality Rate
		15  Lung Cancer
		16  Prostate Cancer in Males
		17  Stroke (Cerebrovascular Disease)
		18  Childhood Blood Lead Level Screening
		19  Childhood Lead Poisoning
		20  Gonorrhea in Females
		21  Gonorrhea in Males
		22  Tuberculosis
		23  Below Poverty Level
		24  Crowded Housing
		25  Dependency
		26  No High School Diploma
		27  Per Capita Income
		28  Unemployment
		*/
		
		private static int economicCommunityAreaColumn = 0;
		private static int[] relevantEconomicColumns = {2,3,4,5,6};
		private static String[] relevantEconomicColumnNames = {"CROWDED","BELOW_POVERTY","16+_UNEMPLOYED","25+_NO_HIGHSCHOOL","YOUR_OR_OLDER"};
		
		/*
		0 Community Area Number
		1 COMMUNITY AREA NAME
		2 PERCENT OF HOUSING CROWDED
		3 PERCENT HOUSEHOLDS BELOW POVERTY
		4 PERCENT AGED 16+ UNEMPLOYED
		5 PERCENT AGED 25+ WITHOUT HIGH SCHOOL DIPLOMA
		6 PERCENT AGED UNDER 18 OR OVER 64
		7 PER CAPITA INCOME 
		8 HARDSHIP INDEX
		*/
		
		
		@Override
		public void setup(Context context){
			
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().toString();
			
			if(filename.contains("weather")){
				
				fileType = FILE_TYPE.WEATHER_FILE;
				sdf = new SimpleDateFormat("yyyyMMdd");
				
			}else if(filename.contains("health")){
				fileType = FILE_TYPE.HEALTH_FILE;
				
			}else if(filename.contains("economic")){
				fileType = FILE_TYPE.ECONOMIC_FILE;
				
			}else{
				fileType = FILE_TYPE.CRIME_FILE;
				sdf = new SimpleDateFormat("MM/dd/yyyy");
			}
		}
		
		@Override
		public void map(LongWritable byteOffset, Text line, Context context) throws IOException, InterruptedException{
			
			// Split the line
			String[] parts;
			
			switch(fileType){
			
				case WEATHER_FILE:
					if(byteOffset.get() == 0L)return;
				
					// Split the line
					parts = line.toString().split(",");
										
					try{
						// Try to parse the date
						date = sdf.parse(parts[dateColumn]);
					}catch(ParseException e){
						//e.printStackTrace();
						// Failed to parse date, return.
						return;
					}
					millis = date.getTime();
					System.out.println("RelevantColumns="+Arrays.toString(relevantWeatherColumns));
					// We want to write all these columns for every of the 50 top terms
					for(int columnNumber : relevantWeatherColumns){
						for (byte i=0;i<50;i++){
							//System.out.printf("%s -> (%d,%s,%d)","t"+i+"w"+columnNumber,millis, DateTypeValue.weatherPrefix+columnNumber,i);
							context.write(new Text("t"+i+"w"+columnNumber), new DateTypeValue(millis,DateTypeValue.weatherPrefix+columnNumber,Float.valueOf(parts[columnNumber])));
						}
					}
				break;
				case CRIME_FILE:
					
					Crime c = GlobalFunctions.parseCrime(line.toString());
					millis = c.getDate().get();
					// Write every frequency with the community area # as the key
					for(byte crimeRanking=1;crimeRanking<=TopCrimes.NUMBER_OF_CRIMES;crimeRanking++){
						// One for every potential weather indicator
						for(int weatherColumn : relevantWeatherColumns){
							//System.out.printf("%s -> (%d,%s,%d)","t"+currentTop50+"w"+weatherColumn,millis, DateTypeValue.top50Prefix, currentTop50);
							//context.write(new Text("C"+crimeRanking+"W"+relevantWeatherColumnNames[weatherColumn]), new DateTypeValue(millis, DateTypeValue.top50Prefix, Float.valueOf(frequencies[currentTop50])));
						}
					}
				break;
				case HEALTH_FILE:
					if(byteOffset.get() == 0L)return;
					
					// Split the line
					parts = line.toString().split(",");
					
					
					
				break;
				case ECONOMIC_FILE:
					if(byteOffset.get() == 0L)return;
					
					// Split the line
					parts = line.toString().split(",");
					
				break;
				
				default:
					return;
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
        
        correlationJob.setJobName("Correlate Weather and Demographics to Daily Crime Frequencies");
        
        correlationJob.setMapOutputKeyClass(Text.class);
        correlationJob.setMapOutputValueClass(DateTypeValue.class);
		
        correlationJob.setOutputKeyClass(DoubleWritable.class);
        correlationJob.setOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(correlationJob, new Path(options[0]));
        Path outputPath = new Path(options[1]);
		if(fs.exists(outputPath)){
	    	fs.delete(outputPath,true);
	    }
		
        FileOutputFormat.setOutputPath(correlationJob, outputPath);
        
        System.exit(correlationJob.waitForCompletion(true) ? 1 : 0);
        /**/
	}

}
