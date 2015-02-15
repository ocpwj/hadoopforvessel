package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v3sortbyshipandtime;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator_ShipID extends WritableComparator {

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		Key_ShipIDAndRecordTime k1 = (Key_ShipIDAndRecordTime)a;
		Key_ShipIDAndRecordTime k2 = (Key_ShipIDAndRecordTime)b;
		
		return k1.getShipID().compareTo(k2.getShipID());
		
	}

	public GroupComparator_ShipID()
	{
		super(Key_ShipIDAndRecordTime.class,true);
	}
	
	
}
