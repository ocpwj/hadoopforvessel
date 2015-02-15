package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v2combinefileinputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class VTVesselLocationTest extends Configured implements Tool {
	

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			printUsage(this, "<input> <output>");
			return -1;
		}
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(this.getClass());
		job.setJobName("test");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setInputFormatClass(VTVesselLocationFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//SequenceFileOutputFormat.setCompressOutput(job, true);
		//SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(VesselLocationMapper.class);
		//job.setNumReduceTasks(0);
		return job.waitForCompletion(true) ? 0 : 1;
		
		
	}
	
	public static void printUsage(Tool tool, String extraArgsUsage) {
		System.err.printf("Usage: %s [genericOptions] %s\n\n",
				tool.getClass().getSimpleName(), extraArgsUsage);
		GenericOptionsParser.printGenericCommandUsage(System.err);
	}
	
	public static void main(String[] args)
	{

		
		try {
			System.out.println(System.getProperty("java.library.path"));
			
			int exitCode = ToolRunner.run(new VTVesselLocationTest(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
