package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v3sortbyshipandtime;

import org.apache.hadoop.mapreduce.Partitioner;

public class Partitioner_ShipID extends Partitioner<Key_ShipIDAndRecordTime, TextArrayWritable> {

	@Override
	public int getPartition(Key_ShipIDAndRecordTime key, TextArrayWritable value,
			int numReduceTasks) {
		// TODO Auto-generated method stub
		
		int hashCode=key.getShipID().hashCode();		
		return (hashCode & Integer.MAX_VALUE) % numReduceTasks;
		
	}

}

