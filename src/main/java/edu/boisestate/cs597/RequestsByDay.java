package edu.boisestate.cs597;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

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

public class RequestsByDay
{
	
	public static class RequestsByDayMap extends Mapper<LongWritable, Text, Text, Text>
	{
		Pattern pattern = Pattern.compile(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		int date = 1;
		int requestType = 5;
		
		@Override
		public void map(LongWritable byteOffset, Text lineFromFile, Context context) throws IOException, InterruptedException
		{
			String[] split = pattern.split(lineFromFile.toString(), -1);
			try
			{
				context.write(new Text(split[date].split(" ")[0]), new Text(split[requestType]));	
			}
			catch (ArrayIndexOutOfBoundsException e)
			{
				return;
			}
				
		}
	}

	public static class RequestsByDayReduce extends Reducer<Text, Text, Text, Text>
	{
		LinkedList<String> top50 = new LinkedList<>();
		@Override
		public void setup(Context context) throws IOException, InterruptedException
		{
			top50 = GlobalFunctions.parseTop50(context.getConfiguration().get("top50"));
		}
		
		@Override
		public void reduce(Text date, Iterable<Text> requests, Context context) throws IOException, InterruptedException
		{
			Map<String, Integer> requestsPerDay = new HashMap<String, Integer>();
			int[] freqVector = new int[50];
			Arrays.fill(freqVector, 0);
			
			for(Text text : requests)
			{
				String request = text.toString();
				if (requestsPerDay.get(request) == null && top50.contains(request))
				{
					requestsPerDay.put(text.toString(), 1);
				}
				else if (top50.contains(request))
				{
					Integer count = requestsPerDay.get(request);
					requestsPerDay.put(request, ++count);
				}
			}
			for (Entry<String, Integer> entry : requestsPerDay.entrySet())
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
		Job requestsByDate = new Job(conf);
		requestsByDate.setJarByClass(RequestsByDay.class);
		requestsByDate.setJobName("Get Requests By Date");
		requestsByDate.setOutputKeyClass(Text.class);
		requestsByDate.setOutputValueClass(Text.class);
		requestsByDate.setMapperClass(RequestsByDayMap.class);
		requestsByDate.setReducerClass(RequestsByDayReduce.class);
		FileInputFormat.setInputPaths(requestsByDate, new Path(options[0]));
		FileOutputFormat.setOutputPath(requestsByDate, new Path(options[1]));
		System.exit(requestsByDate.waitForCompletion(true) ? 1 : 0);
	}
}
