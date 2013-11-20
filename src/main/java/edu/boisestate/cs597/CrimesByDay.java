package edu.boisestate.cs597;

import comparators.DateComparator;
import edu.boisestate.cs597.model.Crime;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

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

    public static class CrimesByDayReduce extends Reducer<Text, Crime, Text, Text> {

        private LinkedList<String> top50 = new LinkedList<>();
        private final int NUM_CRIMES = 50;

        @Override
        public void setup(Context context) throws IOException, InterruptedException
        {
            top50 = GlobalFunctions.parseTop50(context.getConfiguration().get("top50"));
            System.out.println(top50.toString());
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
                freqVector[top50.indexOf(entry.getKey())] = entry.getValue();
            }
            context.write(new Text(date), new Text(Arrays.toString(freqVector)));
        }
    }

    public static void main(String[] args) throws Exception
    {
        GenericOptionsParser gop = new GenericOptionsParser(args);
        String[] options = gop.getRemainingArgs();

        Configuration conf = gop.getConfiguration();
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
        FileInputFormat.setInputPaths(crimesByDay, new Path(options[0]));
        FileOutputFormat.setOutputPath(crimesByDay, new Path(options[1]));
        System.exit(crimesByDay.waitForCompletion(true) ? 1 : 0);
    }
    
    public static void usage()
    {
        System.err.println("usage: CrimesByDay <inputFile> <outputDir> <top50csv>");
        System.exit(1);
    }
}
