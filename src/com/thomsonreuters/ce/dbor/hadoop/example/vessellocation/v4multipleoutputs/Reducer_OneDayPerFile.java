package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v4multipleoutputs;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.VLongWritable;


public class Reducer_OneDayPerFile extends
		Reducer<Key_ShipIDAndRecordTime, TextArrayWritable, VLongWritable, Text> {
	
	private MultipleOutputs<VLongWritable, Text> multipleOutputs;
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd"); 
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<VLongWritable, Text>(context);
	}
	
	@Override
	public void reduce(Key_ShipIDAndRecordTime key, Iterable<TextArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		
		
		String dateString = formatter.format(new Date(key.getRecordTime().get())); 
		
		for (TextArrayWritable value : values) {
			
			Writable[] allfields=value.get();
			 String all=null;
			 
			 for (Writable col:allfields)
			 {
				 if (all!=null)
				 {
					 all=all + "|"+ ((Text)col).toString();
				 }
				 else
				 {
					 all=((Text)col).toString();
				 }
			 }
			 			 
			 multipleOutputs.write("DATAPERYEAR",key.getShipID(), new Text(all), dateString);
		}

	}
	
	@Override
	protected void cleanup(Context context)	throws IOException, InterruptedException {
		multipleOutputs.close();
	}

}
