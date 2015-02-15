package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v5text2darray;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator_ShipID extends WritableComparator {
	
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd"); 

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		Key_ShipIDAndRecordTime k1 = (Key_ShipIDAndRecordTime)a;
		Key_ShipIDAndRecordTime k2 = (Key_ShipIDAndRecordTime)b;
		
		
		VLongWritable ShipID1=k1.getShipID();
		VLongWritable recordTime1=k1.getRecordTime();
		String dateString1 = formatter.format(new Date(recordTime1.get())); 
		
		VLongWritable ShipID2=k2.getShipID();
		VLongWritable recordTime2=k2.getRecordTime();
		String dateString2 = formatter.format(new Date(recordTime2.get())); 
		
		int cmp = ShipID1.compareTo(ShipID2);
		
		if (cmp != 0) {
			return cmp;
		}		
		
		return dateString1.compareTo(dateString2);
		
	}

	public GroupComparator_ShipID()
	{
		super(Key_ShipIDAndRecordTime.class,true);
	}
	
	
}
