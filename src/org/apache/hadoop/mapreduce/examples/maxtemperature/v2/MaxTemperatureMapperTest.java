package org.apache.hadoop.mapreduce.examples.maxtemperature.v2;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class MaxTemperatureMapperTest {

	@Test
	public void processesValidRecord() throws IOException, InterruptedException {
		Text value = new Text(
				"0043011990999991950051518004+68750+023550FM-12+0382" +
				// Year ^^^^
						"99999V0203201N00261220001CN9999999N9-00111+99999999999");
		// Temperature ^^^^^
		MapDriver<LongWritable, Text, Text, IntWritable> mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.withMapper(new MaxTemperatureMapper());
		mapDriver.withInput(new LongWritable(), value);
		mapDriver.withOutput(new Text("1950"), new IntWritable(-11));
		mapDriver.runTest();
	}

	@Test
	public void ignoresMissingTemperatureRecord() throws IOException,
			InterruptedException {
		Text value = new Text(
				"0043011990999991950051518004+68750+023550FM-12+0382" +
				// Year ^^^^
						"99999V0203201N00261220001CN9999999N9+99991+99999999999");
		// Temperature ^^^^^
		MapDriver<LongWritable, Text, Text, IntWritable> mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.withMapper(new MaxTemperatureMapper());
		mapDriver.withInput(new LongWritable(), value);
		mapDriver.runTest();
	}

	@Test
	public void returnsMaximumIntegerInValues() throws IOException,
			InterruptedException {
		ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver=new ReduceDriver<Text, IntWritable, Text, IntWritable>();
		reduceDriver.withReducer(new MaxTemperatureReducer());
		reduceDriver.withInput(new Text("1950"),Arrays.asList(new IntWritable(10), new IntWritable(5)));
		reduceDriver.withOutput(new Text("1950"), new IntWritable(10));
		reduceDriver.runTest();
	}
}
