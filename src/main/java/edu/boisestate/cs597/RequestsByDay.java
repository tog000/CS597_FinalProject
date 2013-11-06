package edu.boisestate.cs597;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
	

	public static class Top50Map extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		Pattern pattern = Pattern.compile(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
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
	
	public static class GlobalFunctions 
	{
		//returns requests by count in descending order
		public static LinkedList<String> parseTop50(String file) throws IOException
		{
			LinkedList<String> requests = new LinkedList<>();
			Path path = new Path(file);
			FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
			InputStream is = (fs.open(new Path(path.toUri().toString())));
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, Charset.defaultCharset())))
			{
				String line;
				Pattern pattern = Pattern.compile("\\t");
				while ((line = reader.readLine()) != null)
				{
					String[] split = pattern.split(line);
					requests.addLast(split[0]);
				}
			}
			return requests;
		}
	}

	public static void main(String[] args) throws Exception
	{
		GenericOptionsParser gop = new GenericOptionsParser(args);
		String[] options = gop.getRemainingArgs();
//
//        Job top50 = new Job(new Configuration());
//        top50.setJarByClass(RequestsByDay.class);
//        top50.setJobName("Get Top 50");
//        top50.setOutputKeyClass(Text.class);
//        top50.setOutputValueClass(IntWritable.class);
//        top50.setMapperClass(Top50Map.class);
//        top50.setReducerClass(Top50Reduce.class);
//        FileInputFormat.setInputPaths(top50, new Path(options[0]));
////        FileOutputFormat.setOutputPath(top50, new Path("/tmp/top50temp/"));
//        FileOutputFormat.setOutputPath(top50, new Path(options[1]));
//        top50.waitForCompletion(true);

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
