package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v6aggregration.aggregation;

import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



import com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v6aggregration.preparation.Key_ShipIDAndRecordTime;
import com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v6aggregration.preparation.Text2DArrayWritable;

public class Mapper_ConvertKeytoShipIDAndRecordTime extends
		Mapper<VLongWritable, Text2DArrayWritable, Key_ShipIDAndRecordTime, Text2DArrayWritable> {
	
	private final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	
	protected void map(VLongWritable key, Text2DArrayWritable value, Context context)
	        throws IOException, InterruptedException {
		
		Writable[][] alldailyrows=value.get();
		
		String thefirstrecordtime=((Text)alldailyrows[0][21]).toString().trim();
		String recordTime=thefirstrecordtime.substring(0, 19);
		
		ParsePosition pos = new ParsePosition(0);
		long record_time=formatter.parse(recordTime, pos).getTime();
		
		Key_ShipIDAndRecordTime CombineKey= new Key_ShipIDAndRecordTime();
		
		CombineKey.set(key, new VLongWritable(record_time));
		
		context.write(CombineKey, value);
	}

}
