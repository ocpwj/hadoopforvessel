package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v5text2darray;

import org.apache.hadoop.mapreduce.Partitioner;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Partitioner_Day extends Partitioner<Key_ShipIDAndRecordTime, TextArrayWritable> {

	private SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd"); 
	
	@Override
	public int getPartition(Key_ShipIDAndRecordTime key, TextArrayWritable value,
			int numReduceTasks) {
		// TODO Auto-generated method stub
		
		String dateString = formatter.format(new Date(key.getRecordTime().get())); 

		int hashcode=Integer.parseInt(dateString);
		return (hashcode & Integer.MAX_VALUE) % numReduceTasks;

	}

}

