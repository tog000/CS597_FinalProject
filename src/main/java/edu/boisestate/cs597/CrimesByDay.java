package edu.boisestate.cs597;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.boisestate.cs597.model.Crime;
import edu.boisestate.cs597.util.GlobalFunctions;

public class CrimesByDay {

    public static class CrimesByDayMap extends Mapper<LongWritable, Text, Text, Crime> {

        @Override
        public void map(LongWritable byteOffset, Text lineFromFile, Context context) throws IOException, InterruptedException
        {
            Crime c = GlobalFunctions.parseCrime(lineFromFile.toString());
            try
            {
                context.write(new Text(GlobalFunctions.getMDYfromMillis(c.getDate().get())), c.clone());
                System.out.println(GlobalFunctions.getMDYfromMillis(c.getDate().get()) + " " + (c.clone()).toString());
            }
            catch (CloneNotSupportedException | NullPointerException e)
            {
                System.err.println("Error while mapping crime");
                e.printStackTrace();
                return;
            }
        }
    }

    public static class CrimesByDayReduce extends Reducer<Text, Crime, NullWritable, Crime> {

        private LinkedList<String> top50 = new LinkedList<>();
        private final int NUM_CRIMES = 50;
        private SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");

        @Override
        public void setup(Context context) throws IOException, InterruptedException
        {
            top50 = GlobalFunctions.parseTop50(context.getConfiguration().get("top50"));
            System.out.println("TOP 50 CRIMES="+top50.toString());
        }

        @Override
        public void reduce(Text date, Iterable<Crime> crimes, Context context) throws IOException, InterruptedException
        {
            Map<String, Integer> crimesPerDay = new HashMap<String, Integer>();
            int[] freqVector = new int[NUM_CRIMES];

            for (Crime crime : crimes)
            {
                String iucr = crime.getIUCR().toString();
                if (crimesPerDay.get(iucr) == null && top50.contains(iucr))
                {
                    crimesPerDay.put(iucr, 1);
                }
                else if (top50.contains(iucr))
                {
                    Integer count = crimesPerDay.get(iucr);
                    crimesPerDay.put(iucr, ++count);
                }
            }
            for (Entry<String, Integer> entry : crimesPerDay.entrySet())
            {
                //freqVector[top50.indexOf(entry.getKey())] = entry.getValue();
                Crime crimeFrequency = new Crime();
                try {
					crimeFrequency.setDate(sdf.parse(date.toString()));
					crimeFrequency.setIUCR(entry.getKey());
					crimeFrequency.setFrequency(entry.getValue());

					context.write(NullWritable.get(), crimeFrequency);
					
				} catch (ParseException e) {
					e.printStackTrace();
				}
            }
            //context.write(new Text(date), new Text(Arrays.toString(freqVector)));
        }
    }

    public static void main(String[] args) throws Exception
    {
        GenericOptionsParser gop = new GenericOptionsParser(args);
        String[] options = gop.getRemainingArgs();
        
        Configuration conf = gop.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        
        conf.set("top50", options[2]);
        Job crimesByDay = new Job(conf);
        crimesByDay.setJarByClass(CrimesByDay.class);
        crimesByDay.setJobName("Get Crimes By Date");
        
        crimesByDay.setOutputKeyClass(Text.class);
        crimesByDay.setOutputValueClass(Text.class);
        crimesByDay.setMapOutputKeyClass(Text.class);
        crimesByDay.setMapOutputValueClass(Crime.class);
        
        crimesByDay.setMapperClass(CrimesByDayMap.class);
        crimesByDay.setReducerClass(CrimesByDayReduce.class);
//        crimesByDay.setGroupingComparatorClass(DateComparator.CrimeDateGrouper.class);
//        crimesByDay.setSortComparatorClass(DateComparator.CrimeDateComparator.class);
        
        Path inputPath = new Path(options[1]+"*crime*");
//        FileStatus[] fss = fs.listStatus(inputPath);
//        for (FileStatus status : fss) {
//			if(status.getPath().getName().contains("proteins")){
//				proteinsFound = true;
//				System.out.println("Added path \""+status.getPath().getName()+"\"");
//				FileInputFormat.addInputPath(iterJob, status.getPath());
//			}
//		}
        
        
        FileInputFormat.setInputPaths(crimesByDay, new Path(options[0]));
        

        Path outputPath = new Path(options[1]);
		if(fs.exists(outputPath)){
	    	fs.delete(outputPath,true);
	    }
        FileOutputFormat.setOutputPath(crimesByDay, outputPath);
        
        System.exit(crimesByDay.waitForCompletion(true) ? 1 : 0);
    }
    
    public static void usage()
    {
        System.err.println("usage: CrimesByDay <inputFile> <outputDir> <top50csv>");
        System.exit(1);
    }
}
