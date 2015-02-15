package org.apache.hadoop.mapreduce.examples.maxtemperature.v4;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	enum Temperature {
		OVER_30
	}

	private NcdcRecordParser parser = new NcdcRecordParser();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		parser.parse(value);
		if (parser.isValidTemperature()) {
			int airTemperature = parser.getAirTemperature();
			if (airTemperature > 300) {
				System.err.println("Temperature over 30 degrees for input: "
						+ value);
				context.setStatus("Detected possibly corrupt record: see logs.");
				context.getCounter(Temperature.OVER_30).increment(1);
			}
			context.write(new Text(parser.getYear()), new IntWritable(
					airTemperature));
		}
	}

}