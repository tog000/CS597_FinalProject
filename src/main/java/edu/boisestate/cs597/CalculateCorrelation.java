package edu.boisestate.cs597;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
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
		//private String frequencies[];
				
		//
		// For WEATHER files
		//
		private static int dateColumn = 2;

		private static int[] relevantWeatherColumns = {21,22,23,27,20,18};
		private static String[] relevantWeatherColumnNames = {"DAILY_SUNSHINE","TEMP_MAX","TEMP_MIN","WIND_SPEED","SNOW","PRCP"};
		
		/*
		
		0 STATION
		1 STATION_NAME
		2 DATE
		3 MXPN Daily maximum temperature of water in an evaporation pan (tenths of degrees C)
		4 MNPN Daily minimum temperature of water in an evaporation pan (tenths of degrees C)
		5 EVAP Evaporation of water from evaporation pan (tenths of mm)
		6 MDEV Multiday evaporation total (tenths of mm; use with DAEV)
		7 DAEV Number of days included in the multiday evaporation total (MDEV)
		8 SX32 Maximum soil temperature (tenths of degrees C) with bare ground cover at 10 cm depth
		9 SX52 Maximum soil temperature (tenths of degrees C) with sod cover at 10 cm depth
		10 SX53 Maximum soil temperature (tenths of degrees C) with sod cover at 20 cm depth
		11 SN32 Minimum soil temperature (tenths of degrees C) with bare ground cover at 10 cm depth
		12 SN52 Minimum soil temperature (tenths of degrees C) with sod cover at 10 cm depth
		13 SN53 Minimum soil temperature (tenths of degrees C) with sod cover at 20 cm depth
		14 MDPR Multiday precipitation total (tenths of mm; use with DAPR and DWPR, if available)
		15 MDSF Multiday snowfall total
		16 DAPR Number of days included in the multiday precipitation total (MDPR)
		17 DASF Number of days included in the multiday snow fall total (MDSF)
		18 PRCP Precipitation (tenths of mm)
		19 SNWD Snow depth (mm)
		20 SNOW Snowfall (mm)
		21 TSUN Daily total sunshine (minutes)
		22 TMAX Maximum temperature (tenths of degrees C)
		23 TMIN Minimum temperature (tenths of degrees C)
		24 TOBS Temperature at the time of observation (tenths of degrees C)
		25 WESD Water equivalent of snow on the ground (tenths of mm)
		26 WESF Water equivalent of snowfall (tenths of mm)
		27 AWND Average daily wind speed (tenths of meters per second)
		
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
		private static String[] relevantEconomicColumnNames = {"CROWDED","BELOW_POVERTY","16_UNEMPLOYED","25_NO_HIGHSCHOOL","YOUNG_OR_OLDER"};
		
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
				
			}else if(filename.contains("economy")){
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
			int columnNumber = 0;
			
			
			switch(fileType){
			
				case CRIME_FILE:
					Crime c = GlobalFunctions.parseCrime(line.toString());
					millis = c.getDate().get();
					
					System.out.printf("Read a new crime with RANKING:%s and COMMA=%s\n",c.getCrimeRanking(),c.getCommunityArea());
					
					// One for every potential weather indicator
					for(int weatherColumn=0;weatherColumn<relevantWeatherColumns.length;weatherColumn++){
						context.write(new Text("W"+relevantWeatherColumnNames[weatherColumn]+"C"+c.getCrimeRanking()), new DateTypeValue(millis, DateTypeValue.top50Prefix, c.getFrequency().get()));
					}
					
					// One for every health column. The "Date" is the community area
					for(int healthColumn=0;healthColumn<relevantHealthColumns.length;healthColumn++){
						context.write(new Text("H"+relevantHealthColumnNames[healthColumn]+"C"+c.getCrimeRanking()), new DateTypeValue((long)c.getCommunityArea().get(), DateTypeValue.top50Prefix, c.getFrequency().get()));
					}
					
					// One for every economic column. The "Date" is the community area
					for(int economicColumn=0;economicColumn<relevantEconomicColumns.length;economicColumn++){
						context.write(new Text("E"+relevantEconomicColumnNames[economicColumn]+"C"+c.getCrimeRanking()), new DateTypeValue((long)c.getCommunityArea().get(), DateTypeValue.top50Prefix, c.getFrequency().get()));
					}
					
				break;
				case WEATHER_FILE:
					if(byteOffset.get() == 0L)return;
				
					// Split the line
					parts = line.toString().split(",");
										
					try{
						// Try to parse the date
						date = sdf.parse(parts[dateColumn]);
					}catch(ParseException e){
						System.err.println("Error while parsing crime.\nLINE="+line);
						e.printStackTrace();
						// Failed to parse date, return.
						return;
					}
					millis = date.getTime();
					
					// We want to write all these columns for every of the 50 top terms
					for(int weatherColumn=0;weatherColumn<relevantWeatherColumns.length;weatherColumn++){
						columnNumber = relevantWeatherColumns[weatherColumn];
						if(!parts[columnNumber].isEmpty()){
							for(byte crimeRanking=1;crimeRanking<=TopCrimes.NUMBER_OF_CRIMES;crimeRanking++){
								//System.out.printf("%s -> (%d,%s,%d)","t"+i+"w"+columnNumber,millis, DateTypeValue.weatherPrefix+columnNumber,i);
								context.write(new Text("W"+relevantWeatherColumnNames[weatherColumn]+"C"+crimeRanking), new DateTypeValue(millis,DateTypeValue.weatherPrefix+columnNumber,Float.valueOf(parts[columnNumber])));
							}
						}
					}
				break;
				case HEALTH_FILE:
					if(byteOffset.get() == 0L)return;
					
					// Split the line
					parts = line.toString().split(",");
					
					for(int healthColumn=0;healthColumn<relevantHealthColumns.length;healthColumn++){
						columnNumber = relevantHealthColumns[healthColumn];
						if(!parts[columnNumber].isEmpty()){
							for(byte crimeRanking=1;crimeRanking<=TopCrimes.NUMBER_OF_CRIMES;crimeRanking++){
								context.write(new Text("H"+relevantHealthColumnNames[healthColumn]+"C"+crimeRanking), 
										new DateTypeValue(
												Long.valueOf(parts[healthCommunityAreaColumn]).longValue(),
												DateTypeValue.healthPrefix+columnNumber,
												Float.valueOf(parts[columnNumber])
												));
							}
						}
					}
					
				break;
				case ECONOMIC_FILE:
					if(byteOffset.get() == 0L)return;
					
					// Split the line
					parts = line.toString().split(",");
					
					for(int economicColumn=0;economicColumn<relevantEconomicColumns.length;economicColumn++){
						columnNumber = relevantEconomicColumns[economicColumn];
						if(!parts[columnNumber].isEmpty()){
							for(byte crimeRanking=1;crimeRanking<=TopCrimes.NUMBER_OF_CRIMES;crimeRanking++){
								context.write(
										new Text("E"+relevantEconomicColumnNames[economicColumn]+"C"+crimeRanking),
										new DateTypeValue(
												Long.valueOf(parts[economicCommunityAreaColumn]).longValue(),
												DateTypeValue.economyPrefix+columnNumber,
												Float.valueOf(parts[columnNumber])
									));
							}
						}
					}
					
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
		public String toString(){
			String buffer = "";
			buffer += "(";
			if(this.x == null){
				buffer += "null";
			}else{
				buffer += x.toString();
			}
			buffer += ",";
			if(this.y == null){
				buffer += "null";
			}else{
				buffer += y.toString();
			}
			buffer += ")";
			return buffer;
		}
	}
	
	public static class InitialReducer extends Reducer<Text, DateTypeValue, DoubleWritable, Text>{
		
		private HashMap<Long, Point> dateMap = null;
		private Collection<Point> goodPoints = null;
		private MultipleOutputs<DoubleWritable, Text> mos;
		
		@Override
		public void setup(Context context){
			mos = new MultipleOutputs<DoubleWritable, Text>(context);
		}
		
		
		// We must align the dates
		@Override
		public void reduce(Text key, Iterable<DateTypeValue> values, Context context) throws IOException, InterruptedException{
			
			int count = 0;
			
			dateMap = new HashMap<Long, Point>();
			goodPoints = new LinkedList<Point>();
			
			for(DateTypeValue badDtv : values){
				count+=1;
				
				DateTypeValue dtv = null;
				
				try {
					dtv = badDtv.clone();
				} catch (CloneNotSupportedException e) {
					e.printStackTrace();
				}
				
				if(dtv==null){
					continue;
				}
				
				/*if(key.toString().equals("WAWNDC1")){
					System.out.println("--->"+dtv);
				}*/
				
				
				// In this case we match by date
				if(key.toString().startsWith("W")){
					
					if(!dateMap.containsKey(dtv.date.get())){
						dateMap.put(dtv.date.get(),new Point());
					}
					
					if(dtv.isWeather()){
						if(dtv.value.get() != -9999f){
							dateMap.get(dtv.date.get()).y = dtv.value.get();
						}
					}else{
						dateMap.get(dtv.date.get()).x = dtv.value.get();
					}
					
				}else if(key.toString().startsWith("H") || key.toString().startsWith("E")){
					
					if(!dateMap.containsKey(dtv.date.get())){
						dateMap.put(dtv.date.get(),new Point());
					}
					
					if(dtv.isCrimeFrequency()){
						Float x = dateMap.get(dtv.date.get()).x;
						if(x==null){
							dateMap.get(dtv.date.get()).x = new Float(5);
						}else{
							dateMap.get(dtv.date.get()).x += 1;
						}
					}else{
						Float y = dateMap.get(dtv.date.get()).y;
						if(y==null){
							dateMap.get(dtv.date.get()).y = new Float(dtv.value.get());
						}else{
							//dateMap.get(dtv.date.get()).y += dtv.value.get();
						}
					}
					
				}
			}
			
			
			Collection<Point> pointValues = dateMap.values();
			
			LinkedList<Float> xArray = new LinkedList<Float>();
			LinkedList<Float> yArray = new LinkedList<Float>();
			
			String buffer = "";
			
			for(Point p : pointValues){
				if(p.isComplete()){
					goodPoints.add(p);
					xArray.add(p.x);
					yArray.add(p.y);
					mos.write(new DoubleWritable(goodPoints.size()), new Text(p.x+"\t"+p.y), "scatter_"+key.toString());
					//buffer += p.toString()+"|";
				}
				
			}
			
			//System.out.println("For the key \""+key.toString()+"\" we have "+count+". The map has "+pointValues.size()+". Good Points "+goodPoints.size());
			
			double xPrimitive[] = new double[xArray.size()];
			double yPrimitive[] = new double[yArray.size()];
			
			int i=0;
			for(Float v:xArray) xPrimitive[i++] = v;
			i=0;
			for(Float v:yArray) yPrimitive[i++] = v;
			
			SpearmansCorrelation sc = new SpearmansCorrelation();
			
			double rho = 0;
			
			
			if(xPrimitive.length > 1 && yPrimitive.length > 1){
				rho = sc.correlation(xPrimitive, yPrimitive);
			}
			
			//System.out.printf("For %s, freq=%d, weather=%d\n",key, totalFrequencies,totalWeatherPoints);
			//System.out.printf("For %s, good points=%d\n",key, totalGoodPoints);
			//context.write(new DoubleWritable(Math.abs(rho)), new Text(key.toString()+"\t"+rho+"\t"+buffer));
			context.write(new DoubleWritable(Math.abs(rho)), new Text(key.toString()+"\t"+rho+"\t"));		
			
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
		
	}

	public static void main(String[] args) throws Exception{

		//System.out.printf("[%d]",Integer.parseInt("42          ".trim()));
		//System.exit(1);
		
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
        
        
        // Save the raw points for visualization
        //MultipleOutputs.addNamedOutput(correlationJob, "scatter", TextOutputFormat.class, Text.class, NullWritable.class);
        LazyOutputFormat.setOutputFormatClass(correlationJob, TextOutputFormat.class);
        
        correlationJob.setMapOutputKeyClass(Text.class);
        correlationJob.setMapOutputValueClass(DateTypeValue.class);
		
        correlationJob.setOutputKeyClass(DoubleWritable.class);
        correlationJob.setOutputValueClass(Text.class);
        
        // Weather files
        FileInputFormat.addInputPath(correlationJob, new Path(options[0]));
        // Health files
        FileInputFormat.addInputPath(correlationJob, new Path(options[1]));
        // Economy files
        FileInputFormat.addInputPath(correlationJob, new Path(options[2]));
        // Daily Crime files
        FileInputFormat.addInputPath(correlationJob, new Path(options[3]));
        
        Path outputPath = new Path(options[4]);
		if(fs.exists(outputPath)){
	    	fs.delete(outputPath,true);
	    }
		//fs.deleteOnExit(outputPath);
		
        FileOutputFormat.setOutputPath(correlationJob, outputPath);
        
        System.exit(correlationJob.waitForCompletion(true) ? 1 : 0);
        /**/
	}

}
