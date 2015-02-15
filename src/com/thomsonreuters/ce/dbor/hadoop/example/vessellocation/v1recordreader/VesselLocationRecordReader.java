package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v1recordreader;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import au.com.bytecode.opencsv.CSVParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class VesselLocationRecordReader extends RecordReader<IntWritable, ArrayWritable> {

	private IntWritable key= new IntWritable();
	private ArrayWritable value = new ArrayWritable(Text.class);
	private boolean ReachEnd = false;
	private FSDataInputStream in = null;
	private long file_length;
	private long currentpos=0L;
	private CSVParser CSVP=new CSVParser(',','"','\\',true,false);
	private BufferedReader BR;
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public ArrayWritable getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return currentpos/(float)file_length;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {


		FileSplit fileSplit = (FileSplit) split;
		Configuration conf = context.getConfiguration();

		Path file = fileSplit.getPath();

		FileSystem fs = file.getFileSystem(conf);

		in = fs.open(file);
		ZipInputStream zis=new ZipInputStream(in);
		ZipEntry ze=zis.getNextEntry();
		if (ze!=null)
		{
			String FileName = ze.getName();
			file_length=ze.getSize();
			if ((FileName.indexOf("VTCurrentLocation") >= 0 ))
			{
				BR=new BufferedReader(new InputStreamReader(zis));
			}
			else
			{
				ReachEnd=true;
			}
		}
		else
		{
			BR.close();
			IOUtils.closeStream(in);
			ReachEnd=true;
		}

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {


		// TODO Auto-generated method stub
		if (!ReachEnd) {

			String strRow=BR.readLine();
			
			if (strRow!=null)
			{
				String[] nextrow=CSVP.parseLine(strRow);

				currentpos=currentpos+strRow.getBytes().length;
				key.set(Integer.parseInt(nextrow[0].trim()));

				Text[] allfields=new Text[nextrow.length];

				for(int i=0;i<nextrow.length;i++)
				{
					allfields[i]=new Text(nextrow[i]);					
				}

				value.set(allfields);
				return true;
			}
			else
			{
				IOUtils.closeStream(in);
				BR.close();
				ReachEnd=true;
				return false;
			}
		}
		else
		{
			return false;
		}

	}

}
