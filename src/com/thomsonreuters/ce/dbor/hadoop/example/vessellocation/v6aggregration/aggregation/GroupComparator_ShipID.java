package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v6aggregration.aggregation;

import java.util.Date;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v6aggregration.preparation.Key_ShipIDAndRecordTime;

public class GroupComparator_ShipID extends WritableComparator {

	public GroupComparator_ShipID() {
		// TODO Auto-generated constructor stub
		super(Key_ShipIDAndRecordTime.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		Key_ShipIDAndRecordTime k1 = (Key_ShipIDAndRecordTime)a;
		Key_ShipIDAndRecordTime k2 = (Key_ShipIDAndRecordTime)b;
		
		VLongWritable ShipID1=k1.getShipID();
		VLongWritable ShipID2=k2.getShipID();
		return ShipID1.compareTo(ShipID2);
	}
	

}
