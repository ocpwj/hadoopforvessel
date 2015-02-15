package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v6aggregration.eventcalc;


import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import org.geotools.geometry.jts.JTSFactoryFinder;


import com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v6aggregration.preparation.Text2DArrayWritable;

public class Mapper_EventCalculator extends
		Mapper<VLongWritable, Text2DArrayWritable, VLongWritable, EventArrayWritable> {
	
	private HashMap<Integer, VesselZone> Zonemap;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		try {
			File Zonemapfile=new File("zones");
			ObjectInputStream OIS= new ObjectInputStream(new FileInputStream(Zonemapfile));
			Zonemap= (HashMap<Integer, VesselZone>)OIS.readObject();
			OIS.close();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}




	private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

	@Override
	protected void map(VLongWritable ShipID, Text2DArrayWritable value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		HashMap<Integer, Vessel_Event> PreviousZoneEvents =new HashMap<Integer, Vessel_Event>();		
		ArrayList<Vessel_Event> EventDrived= new ArrayList<Vessel_Event>();
		
		Writable[][] alllocationrows=value.get();
		
		String strIMO=((Text)alllocationrows[0][1]).toString().trim();
		
		VLongWritable IMO=new VLongWritable(0);
		
		if (!strIMO.equals(""))
		{
			IMO=new VLongWritable(Long.parseLong(strIMO));
		}
		

		Text VesselName=new Text(alllocationrows[0][7].toString().trim());
		
		/*
	  	private VLongWritable Ship_ID;
		private VLongWritable IMO;
		private Text VesselName;
		private VLongWritable ZoneAxsmarine_ID;
		private Text Zone_Name;	
		private VLongWritable EntryTime;
		private VLongWritable LastTime;
		private BooleanWritable IsClosed;
		 */
		
		for (Writable[] thisrow : alllocationrows)
		{

			double pLong=Double.parseDouble(((Text)thisrow[15]).toString());
			double pLat=Double.parseDouble(((Text)thisrow[16]).toString());
			
			String recordTime=((Text)thisrow[21]).toString().substring(0, 19);
			ParsePosition pos = new ParsePosition(0);
			long record_time=formatter.parse(recordTime, pos).getTime();
			
			ArrayList<Integer> CurrentZones = LocateCurrentZone(pLong,pLat);

			if (CurrentZones.isEmpty())
			{
			   //ignore this coordinates because of no matching polygons
			   continue;
			}
			
			
			Iterator<Map.Entry<Integer, Vessel_Event>> it = PreviousZoneEvents.entrySet().iterator();  
			while (it.hasNext())
			{
				Map.Entry<Integer, Vessel_Event> thisEntry=it.next();
				
				if (!CurrentZones.contains(thisEntry.getKey()))
				{
					Vessel_Event PreviousEvent=thisEntry.getValue();
					PreviousEvent.setIsClosed(new BooleanWritable(true));

					if (!EventDrived.contains(PreviousEvent))
					{
						EventDrived.add(PreviousEvent);
					}
					//remove close event from PreviousZoneEvents;
					it.remove();
				}
			}


			for (Integer thisZone_ID : CurrentZones) {

				if (PreviousZoneEvents.containsKey(thisZone_ID))
				{
					//////////////////////////////////////////////////
					//For current zones which both previous and current locations belong to, update exit point of previous open events with current locations.
					//////////////////////////////////////////////////
					Vessel_Event PreviousEvent=PreviousZoneEvents.get(thisZone_ID);
					PreviousEvent.setLastTime(new VLongWritable(record_time));
					PreviousEvent.setIsClosed(new BooleanWritable(false));

					if (!EventDrived.contains(PreviousEvent))
					{
						EventDrived.add(PreviousEvent);
					}
				}
				else
				{
					//////////////////////////////////////////////////
					//For current zones which only current locations belong to, fire new open events
					//////////////////////////////////////////////////
					
					Vessel_Event NewEvent=new Vessel_Event(ShipID,IMO,VesselName, new VLongWritable(thisZone_ID), new Text(Zonemap.get(thisZone_ID).getName()),new VLongWritable(record_time),new VLongWritable(record_time),new BooleanWritable(false));
					PreviousZoneEvents.put(thisZone_ID, NewEvent);

					EventDrived.add(NewEvent);		

				}
			}
		}
		
		
		Vessel_Event[] VE = new Vessel_Event[EventDrived.size()];
		for (int i=0 ; i<EventDrived.size() ; i++)
		{
			VE[i]=EventDrived.get(i);
		}
		
		context.write(ShipID, new EventArrayWritable(VE));
		
		
	}
	
	
	
	
    private ArrayList<Integer> LocateCurrentZone(double pLong, double pLat)
    {
    	GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
    	Coordinate coord = new Coordinate(pLong, pLat);
    	Point point = geometryFactory.createPoint(coord);

    	ArrayList<Integer> CurrentZones = new ArrayList<Integer>();

    	Integer BelongedGlobalZoneIndex=null;

    	for (int i=0 ; i<VesselZone.GlobalZones.length ; i++)
    	{
    		if (VesselZone.GlobalZones[i].covers(point))
    		{
    			BelongedGlobalZoneIndex=i;
    			break;
    		}
    	}


    	for (Map.Entry<Integer, VesselZone> thisEntry : Zonemap.entrySet()) {

    		VesselZone thisZone=thisEntry.getValue();
    		/*
    		if (thisZone.getZone_type().equals("ZONE"))
    		{
    			if (VesselProductType.equals("Tankers"))
    			{
    				if (!thisZone.HasClassification("TANKER"))
    				{
    					continue;
    				}
    			}
    			else if (VesselProductType.equals("Bulkers"))
    			{
    				if (!thisZone.HasClassification("DRY"))
    				{
    					continue;
    				}
    			}
    			else if (VesselProductType.equals("Container / Roro"))
    			{
    				if (!thisZone.HasClassification("LINER"))
    				{
    					continue;
    				}
    			}
    			else if (VesselProductType.equals("Miscellaneous") || VesselProductType.equals("Passenger"))
    			{
    				continue;
    			}
    		}
    		
    		*/

    		if (thisZone.IntersectedWithGlobalZone(BelongedGlobalZoneIndex))
    		{
    			if (thisZone.getPolygon().covers(point)) {
    				CurrentZones.add(thisZone.getAxsmarine_ID());
    			}	
    		}					
    	}

    	return CurrentZones;
    }		

}
