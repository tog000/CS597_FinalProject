package edu.boisestate.cs597;

import comparators.IucrComparator;
import edu.boisestate.cs597.model.Crime;
import edu.boisestate.cs597.util.GlobalFunctions;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopCrimes {

    public static class TopCrimesMap extends Mapper<LongWritable, Text, Crime, IntWritable> {

        @Override
        public void map(LongWritable byteOffset, Text lineFromFile, Context context) throws IOException, InterruptedException
        {
            Crime c = GlobalFunctions.parseCrime(lineFromFile.toString());
            try
            {
                context.write(c.clone(), new IntWritable(1));
            }
            catch (CloneNotSupportedException | NullPointerException e)
            {
                System.err.println("Error while mapping in top crimes");
                e.printStackTrace();
                return;
            }
        }
    }

    public static class TopCrimesReduce extends Reducer<Crime, IntWritable, Crime, IntWritable> {

        @Override
        public void reduce(Crime cf, Iterable<IntWritable> occurences, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable num : occurences)
            {
                sum += num.get();
            }
            try
            {
                context.write(cf.clone(), new IntWritable(sum));
            }
            catch (CloneNotSupportedException e)
            {
                System.err.println("Error while reducing in top crimes");
                e.printStackTrace();
                return;
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        if (args.length == 0)
        {
            usage();
        }

        GenericOptionsParser gop = new GenericOptionsParser(args);
        String[] options = gop.getRemainingArgs();

        Job topCrimes = new Job(new Configuration());
        topCrimes.setJarByClass(TopCrimes.class);
        topCrimes.setJobName("Get Top 50 Crimes");

        topCrimes.setOutputKeyClass(Crime.class);
        topCrimes.setOutputValueClass(IntWritable.class);
        topCrimes.setMapperClass(TopCrimesMap.class);
        topCrimes.setReducerClass(TopCrimesReduce.class);
        topCrimes.setGroupingComparatorClass(IucrComparator.CrimeIucrGrouper.class);
        topCrimes.setSortComparatorClass(IucrComparator.CrimeIucrComparator.class);

        FileInputFormat.setInputPaths(topCrimes, new Path(options[0]));
        FileOutputFormat.setOutputPath(topCrimes, new Path(options[1]));

        System.exit(topCrimes.waitForCompletion(true) ? 1 : 0);
    }

    public static void usage()
    {
        System.err.println("usage: TopCrimes <inputFile> <outputPath>");
        System.exit(1);
    }
}
