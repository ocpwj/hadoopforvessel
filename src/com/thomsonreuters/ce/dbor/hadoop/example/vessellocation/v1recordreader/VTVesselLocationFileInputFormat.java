package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v1recordreader;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class VTVesselLocationFileInputFormat extends FileInputFormat<IntWritable, ArrayWritable> {
	
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		
		return false;
		
	}

	@Override
	public RecordReader<IntWritable, ArrayWritable> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		VesselLocationRecordReader VLRR=new VesselLocationRecordReader();
		VLRR.initialize(arg0, arg1);
		return VLRR;
	}
	
	

}
