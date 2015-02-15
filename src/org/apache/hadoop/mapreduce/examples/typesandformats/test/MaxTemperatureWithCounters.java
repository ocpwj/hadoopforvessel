package org.apache.hadoop.mapreduce.examples.typesandformats.test;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// vv MaxTemperatureWithCounters
public class MaxTemperatureWithCounters extends Configured implements Tool {
  
  enum Temperature {
    MISSING,
    MALFORMED
  }
  
  static class MaxTemperatureMapperWithCounters
    extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    @Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		
		InputSplit split = context.getInputSplit();
		
		System.out.println("***************Mapper's setup is being executed***************");
		FileSplit FS=(FileSplit) split;
		
		long datastart=FS.getStart();
		System.out.println("***************GetStart() returns " + datastart +" ***************");
		
		long datalongth=FS.getLength();
		System.out.println("***************getLength() returns " + datalongth +" ***************");
		
		String [] datalocations=FS.getLocations();
		System.out.println("***************getLocations() returns " + datalocations.length +" locations***************");
		
		for (int i=0 ; i<datalocations.length; i++)
		{
			System.out.println("***************No." +i+ " location is : " + datalocations[i] +" ***************");
		}
		
		Path path =FS.getPath();
		System.out.println("***************getLocations() returns " + path.toString() +" ***************");
				
	}

	private NcdcRecordParser parser = new NcdcRecordParser();
  
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        int airTemperature = parser.getAirTemperature();
        context.write(new Text(parser.getYear()),
            new IntWritable(airTemperature));
      } else if (parser.isMalformedTemperature()) {
        System.err.println("Ignoring possibly corrupt input: " + value);
        context.getCounter(Temperature.MALFORMED).increment(1);
      } else if (parser.isMissingTemperature()) {
        context.getCounter(Temperature.MISSING).increment(1);
      }
      
      // dynamic counter
      context.getCounter("TemperatureQuality", parser.getQuality()).increment(1);
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
	  
	System.out.println("-------------Printing configuration-------------------");
		
	Configuration conf = getConf();
	for (Entry<String, String> entry : conf) {
			System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
	}
	
	System.out.println("-------------Printing configuration done--------------");
		
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (job == null) {
      return -1;
    }
    
    
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(MaxTemperatureMapperWithCounters.class);
    job.setCombinerClass(MaxTemperatureReducer.class);
    job.setReducerClass(MaxTemperatureReducer.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MaxTemperatureWithCounters(), args);
    System.exit(exitCode);
  }
  
  public static class JobBuilder {
		public static Job parseInputAndOutput(Tool tool, Configuration conf,
				String[] args) throws IOException {
			if (args.length != 2) {
				printUsage(tool, "<input> <output>");
				return null;
			}
			Job job = Job.getInstance(conf);
			job.setJobName("test");
			job.setJarByClass(tool.getClass());
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			return job;
		}

		public static void printUsage(Tool tool, String extraArgsUsage) {
			System.err.printf("Usage: %s [genericOptions] %s\n\n",
					tool.getClass().getSimpleName(), extraArgsUsage);
			GenericOptionsParser.printGenericCommandUsage(System.err);
		}
	}
}