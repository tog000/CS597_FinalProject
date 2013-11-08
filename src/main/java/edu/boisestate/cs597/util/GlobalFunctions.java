package edu.boisestate.cs597.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GlobalFunctions {
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