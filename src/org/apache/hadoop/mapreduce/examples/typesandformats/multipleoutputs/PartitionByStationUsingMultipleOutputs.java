package org.apache.hadoop.mapreduce.examples.typesandformats.multipleoutputs;

//cc PartitionByStationUsingMultipleOutputs Partitions whole dataset into files named by the station ID using MultipleOutputs
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//vv PartitionByStationUsingMultipleOutputs
public class PartitionByStationUsingMultipleOutputs extends Configured
implements Tool {

	static class StationMapper
	extends Mapper<LongWritable, Text, Text, Text> {

		private NcdcRecordParser parser = new NcdcRecordParser();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			parser.parse(value);
			context.write(new Text(parser.getStationId()), value);
		}
	}

	/*[*/static class MultipleOutputsReducer
	extends Reducer<Text, Text, NullWritable, Text> {

		private MultipleOutputs<NullWritable, Text> multipleOutputs;

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				multipleOutputs.write(NullWritable.get(), value, key.toString());
			}
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			multipleOutputs.close();
		}
	}/*]*/

	@Override
	public int run(String[] args) throws Exception {
		Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
		if (job == null) {
			return -1;
		}

		job.setMapperClass(StationMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setReducerClass(MultipleOutputsReducer.class);
		job.setOutputKeyClass(NullWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new PartitionByStationUsingMultipleOutputs(),
				args);
		System.exit(exitCode);
	}

	public static class JobBuilder {
		public static Job parseInputAndOutput(Tool tool, Configuration conf,
				String[] args) throws IOException {
			if (args.length != 2) {
				printUsage(tool, "<input> <output>");
				return null;
			}
			Job job = new Job(conf);
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
//^^ PartitionByStationUsingMultipleOutputs