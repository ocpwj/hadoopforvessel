package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v2combinefileinputformat;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class VesselLocationMapper extends Mapper<IntWritable, ArrayWritable, IntWritable, Text> 
{
	
	 @Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		
		Configuration conf = context.getConfiguration();
		
		System.out.println("Printing configurations:");
		for (Entry<String, String> entry : conf) {
			System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
		}
		System.out.println("Done!");
	}

	protected void map(IntWritable key, ArrayWritable value, Context context)
		        throws IOException, InterruptedException {
		 
		 Text[] allfields=(Text[])value.get();
		 String all=null;
		 
		 for (Text col:allfields)
		 {
			 if (all!=null)
			 {
				 all=all + "|"+ col.toString();
			 }
			 else
			 {
				 all=col.toString();
			 }
		 }
		 
		 context.write(key, new Text(all));
		 
	 }
}