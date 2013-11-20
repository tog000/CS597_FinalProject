package edu.boisestate.cs597;

import java.io.IOException;
import java.util.regex.Pattern;

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

public class Top50Requests {


	public static class Top50Map extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		Pattern pattern = Pattern.compile("");
		int requestType = 5;
		@Override
		public void map(LongWritable byteOffset, Text lineFromFile, Context context) throws IOException, InterruptedException
		{
			String[] split = pattern.split(lineFromFile.toString(), -1);
			try 
			{
				context.write(new Text(split[requestType]), new IntWritable(1));
			}
			catch (ArrayIndexOutOfBoundsException e)
			{
				return;
			}
		}
	}

	public static class Top50Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		@Override
		public void reduce(Text requestType, Iterable<IntWritable> occurences, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable num : occurences)
			{
				sum += num.get();
			}
			context.write(new Text(requestType), new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		GenericOptionsParser gop = new GenericOptionsParser(args);
		String[] options = gop.getRemainingArgs();
		
		Job top50 = new Job(new Configuration());
        top50.setJarByClass(RequestsByDay.class);
        top50.setJobName("Get Top 50");
        top50.setOutputKeyClass(Text.class);
        top50.setOutputValueClass(IntWritable.class);
        top50.setMapperClass(Top50Map.class);
        top50.setReducerClass(Top50Reduce.class);
        FileInputFormat.setInputPaths(top50, new Path(options[0]));
		//FileOutputFormat.setOutputPath(top50, new Path("/tmp/top50temp/"));
        FileOutputFormat.setOutputPath(top50, new Path(options[1]));
        System.exit(top50.waitForCompletion(true) ? 1 : 0);
	}

}
