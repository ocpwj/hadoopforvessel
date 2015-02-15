package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v6aggregration.eventcalc;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class EventArrayWritable extends ArrayWritable {

	public EventArrayWritable() {
		super(Vessel_Event.class);
		// TODO Auto-generated constructor stub
	}

	public EventArrayWritable(Vessel_Event[] events) {
		super(Vessel_Event.class);
		super.set(events);
	}
	
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		Writable[] allEvents=super.get();
		
		String all="\r\nEvents:\r\n";
		int i=0;
		
		for (Writable thisEvent : allEvents)
		{
			i++;			
			all=all+"("+String.valueOf(i)+"):" + ((Vessel_Event)thisEvent).toString() +"\r\n";
		}
		
		return all;
	}	

}
