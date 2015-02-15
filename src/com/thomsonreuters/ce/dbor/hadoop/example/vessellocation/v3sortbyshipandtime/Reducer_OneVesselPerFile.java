package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v3sortbyshipandtime;


import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Reducer_OneVesselPerFile extends
		Reducer<Key_ShipIDAndRecordTime, TextArrayWritable, Text, NullWritable> {
	
	@Override
	public void reduce(Key_ShipIDAndRecordTime key, Iterable<TextArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		
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
			 
			 
			 context.write(new Text(all), NullWritable.get());
		}

	}

}
