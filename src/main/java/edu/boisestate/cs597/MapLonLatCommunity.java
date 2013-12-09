package edu.boisestate.cs597;

import java.awt.geom.Path2D;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.boisestate.cs597.model.Crime;
import edu.boisestate.cs597.util.GlobalFunctions;

public class MapLonLatCommunity {

    public static class InitialMapper extends Mapper<LongWritable, Text, Crime, Text> {

        private HashMap<Integer, Path2D.Double> communityPolygons = new HashMap<Integer, Path2D.Double>();  

        @Override
        public void setup(Context context) throws IOException
        {
            Configuration jobConfig = context.getConfiguration();
            String kmlFile = jobConfig.get("kml_location", "");
            communityPolygons = GlobalFunctions.extractPolygons(kmlFile);
        }

        @Override
        public void map(LongWritable byteOffset, Text line, Context context) throws IOException, InterruptedException
        {
            if (byteOffset.get() == 0L)
            {
                return; //skip schema
            }
            try
            {
                Crime c = GlobalFunctions.parseInitialCrime(line.toString(), communityPolygons);
                context.write(c.clone(), new Text(""));
            }
            catch (CloneNotSupportedException | NullPointerException ex)
            {
                ex.printStackTrace();
                return;
            }
        }
    }

    public static void main(String args[]) throws Exception
    {
        if (args.length == 0) usage();
        
        GenericOptionsParser gop = new GenericOptionsParser(args);
        String[] options = gop.getRemainingArgs();

        Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(conf);
        
        Job lonLatConversionJob = new Job(new Configuration());

        lonLatConversionJob.setNumReduceTasks(4);
        
        lonLatConversionJob.setJobName("Find the community where the crime was commited");
        lonLatConversionJob.setJarByClass(MapLonLatCommunity.class);
        
        lonLatConversionJob.setMapperClass(InitialMapper.class);
        lonLatConversionJob.setMapOutputKeyClass(Crime.class);
        lonLatConversionJob.setMapOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(lonLatConversionJob, new Path(options[0]));
        Path outputPath = new Path(options[1]);
		if(fs.exists(outputPath)){
	    	fs.delete(outputPath,true);
	    }
		
        FileOutputFormat.setOutputPath(lonLatConversionJob, outputPath);
        
        Configuration jobConfig = lonLatConversionJob.getConfiguration();
        jobConfig.set("kml_location", options[2]);

        System.exit(lonLatConversionJob.waitForCompletion(true) ? 1 : 0);
    }
    
    public static void usage()
    {
        System.err.println("usage: MapLatLonCommunity <inputFile> <outputPath> <kmlFile>");
        System.exit(1);
    }

}
