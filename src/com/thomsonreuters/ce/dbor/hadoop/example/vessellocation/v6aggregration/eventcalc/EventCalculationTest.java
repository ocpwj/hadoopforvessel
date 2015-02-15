package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v6aggregration.eventcalc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineSequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class EventCalculationTest extends Configured implements Tool {

	public EventCalculationTest() {
		// TODO Auto-generated constructor stub
	}

	public EventCalculationTest(Configuration conf) {
		super(conf);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			printUsage(this, "<input> <output>");
			return -1;
		}
		
		Configuration conf = getConf();
		conf.set("mapred.max.split.size", "67108864");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName("EventCalculationTest");
		FileInputFormat.addInputPaths(job,args[0]);
		//FileInputFormat.setInputPathFilter(job, DatafileOnly.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setInputFormatClass(CombineSequenceFileInputFormat.class);
		job.setMapperClass(Mapper_EventCalculator.class);
		job.setMapOutputKeyClass(VLongWritable.class);
		job.setMapOutputValueClass(EventArrayWritable.class);
		
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(VLongWritable.class);
		job.setOutputValueClass(EventArrayWritable.class);
;

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
			int exitCode = ToolRunner.run(new EventCalculationTest(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
