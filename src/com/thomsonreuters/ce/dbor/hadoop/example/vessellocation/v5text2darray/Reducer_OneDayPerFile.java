package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v5text2darray;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.VLongWritable;



public class Reducer_OneDayPerFile extends
		Reducer<Key_ShipIDAndRecordTime, TextArrayWritable, VLongWritable, Text2DArrayWritable> {
	
	private MultipleOutputs<VLongWritable, Text2DArrayWritable> multipleOutputs;
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd"); 
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<VLongWritable, Text2DArrayWritable>(context);
	}
	
	@Override
	public void reduce(Key_ShipIDAndRecordTime key, Iterable<TextArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		
		
		String dateString = formatter.format(new Date(key.getRecordTime().get())); 
		
		int i=0;
		
		Writable[][] allrows=null;
		
		for (TextArrayWritable value : values) 
		{	
			i++;
			Writable[] allfields=value.get();

			if (i!=1)
			{
				Writable[][]  tempallrows=new Writable[i][24];

				for (int l=0 ; l<i-1;l++)
				{
					System.arraycopy(allrows[l],0,tempallrows[l],0,24);
				}
				
				int fieldnum=allfields.length;
				if (fieldnum>24)
				{
					fieldnum=24;
				}
								
				System.arraycopy(allfields, 0, tempallrows[i-1], 0, fieldnum);
				
				/*
				try {
					
				} catch (java.lang.ArrayIndexOutOfBoundsException e) {
					// TODO Auto-generated catch block
					
					System.out.println("****************************************");	
					System.out.println("value.toString(): "+value.toString() );
					System.out.println("allfields.length: "+ allfields.length);
					System.out.println("i: "+ i);
					System.out.println("[i-1]: " + String.valueOf(i-1));
					System.out.println("tempallrows[i-1]: "+ tempallrows[i-1].length);
					System.out.println("fieldnum: "+ fieldnum);
					System.out.println("****************************************");
					throw e;
				}*/
				
				allrows=tempallrows;

			}
			else
			{
				
				
				allrows=new Writable[1][24];
				
				int fieldnum=allfields.length;
				if (fieldnum>24)
				{
					fieldnum=24;
				}
				
				System.arraycopy(allfields, 0, allrows[0], 0, fieldnum);
			}

		}
		
		Text2DArrayWritable Text2DArray=new Text2DArrayWritable(allrows);		
		
		multipleOutputs.write("DATAPERDAY",key.getShipID(),Text2DArray, dateString);

	}
	
	@Override
	protected void cleanup(Context context)	throws IOException, InterruptedException {
		multipleOutputs.close();
	}

}
