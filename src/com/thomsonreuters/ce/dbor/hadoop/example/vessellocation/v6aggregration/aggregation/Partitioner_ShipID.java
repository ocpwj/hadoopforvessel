package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v6aggregration.aggregation;


import org.apache.hadoop.mapreduce.Partitioner;

import com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v6aggregration.preparation.Key_ShipIDAndRecordTime;
import com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v6aggregration.preparation.Text2DArrayWritable;

public class Partitioner_ShipID extends Partitioner<Key_ShipIDAndRecordTime, Text2DArrayWritable> {

	
	@Override
	public int getPartition(Key_ShipIDAndRecordTime key, Text2DArrayWritable value,
			int numReduceTasks) {
		// TODO Auto-generated method stub
		
		return (key.getShipID().hashCode() & Integer.MAX_VALUE) % numReduceTasks;

	}
}
