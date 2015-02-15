package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v2combinefileinputformat;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit; 

public class VTVesselLocationFileInputFormat extends CombineFileInputFormat<IntWritable, ArrayWritable> {

	public VTVesselLocationFileInputFormat(){
		super();
		setMaxSplitSize(67108864); // 64 MB, default block size on hadoop
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {

		return false;

	}

	@Override
	public RecordReader<IntWritable, ArrayWritable> createRecordReader(InputSplit split,TaskAttemptContext context) throws IOException
	{
		// TODO Auto-generated method stub
		return new CombineFileRecordReader<IntWritable, ArrayWritable>((CombineFileSplit)split, context, VesselLocationRecordReader.class);

	}

}
