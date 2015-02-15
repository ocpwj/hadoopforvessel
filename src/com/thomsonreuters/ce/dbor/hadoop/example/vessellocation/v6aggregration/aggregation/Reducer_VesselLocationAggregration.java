package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v6aggregration.aggregation;

import java.io.IOException;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v6aggregration.preparation.Key_ShipIDAndRecordTime;
import com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v6aggregration.preparation.Text2DArrayWritable;

public class Reducer_VesselLocationAggregration extends
		Reducer<Key_ShipIDAndRecordTime, Text2DArrayWritable, VLongWritable, Text2DArrayWritable> {

	@Override
	protected void reduce(Key_ShipIDAndRecordTime arg0,	Iterable<Text2DArrayWritable> arg1, Context arg2) throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		VLongWritable shipID=arg0.getShipID();
		
		int i=0;
		Writable[][] allrows=null;
		
		for (Text2DArrayWritable thisDailyValue : arg1 )
		{
			Writable[][] dailyrows=thisDailyValue.get();
			for (Writable[] thisrow: dailyrows)
			{
				i++;
				Writable[][] tempdailyrows= new Writable[i][24];
				for (int l=0; l < i-1 ; l++)
				{
					System.arraycopy(allrows[l], 0, tempdailyrows[l], 0, 24);
				}
				System.arraycopy(thisrow, 0, tempdailyrows[i-1], 0, 24);
				allrows=tempdailyrows;
			}
		}
		
		arg2.write(shipID, new Text2DArrayWritable(allrows));
		
	}	
}
