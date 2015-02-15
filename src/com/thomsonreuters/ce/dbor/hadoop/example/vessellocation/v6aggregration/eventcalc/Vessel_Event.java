package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v6aggregration.eventcalc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BooleanWritable;

public class Vessel_Event implements Writable {
	

	private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		
		String strShip_ID="Ship ID: " + Ship_ID.toString();
		String strIMO="IMO: " + IMO.toString();
		String strVesselName="Vessel Name: "+VesselName.toString();
		String strZoneAxsmarineID="Axsmarine Zone ID: "+ ZoneAxsmarine_ID.toString();
		String strZone_Name="Zone Name: "+ Zone_Name.toString();
		String strEntryTime="Entry Time: "+formatter.format(new Date(EntryTime.get()));
		String strLastTime="Last Time: "+formatter.format(new Date(LastTime.get()));
		
		String strIsClosed;
		if (IsClosed.get())
		{
			strIsClosed="Is Closed: true";
		}
		else
		{
			strIsClosed="Is Closed: false";
		}
		
		String all="["+strShip_ID + " | "+ strIMO + " | "+ strVesselName + "]"+ 
				"["+ strZoneAxsmarineID + " | " + strZone_Name + "]"+ 
				"["+strEntryTime + " | " + strLastTime + "]"+ 
				"["+strIsClosed + "]";
				
		return all;
	}

	private VLongWritable Ship_ID=new VLongWritable() ;
	private VLongWritable IMO=new VLongWritable();
	private Text VesselName=new Text();
	private VLongWritable ZoneAxsmarine_ID=new VLongWritable();
	private Text Zone_Name=new Text();
	private VLongWritable EntryTime=new VLongWritable();
	private VLongWritable LastTime=new VLongWritable();
	private BooleanWritable IsClosed=new BooleanWritable();

	public Vessel_Event() {
		// TODO Auto-generated constructor stub
	}
	
	public Vessel_Event(VLongWritable ship_ID, VLongWritable iMO,
			Text vesselName, VLongWritable zoneAxsmarine_ID, Text zone_Name,
			VLongWritable entryTime, VLongWritable lastTime,
			BooleanWritable isClosed) {
		super();
		Ship_ID = ship_ID;
		IMO = iMO;
		VesselName = vesselName;
		ZoneAxsmarine_ID = zoneAxsmarine_ID;
		Zone_Name = zone_Name;
		EntryTime = entryTime;
		LastTime = lastTime;
		IsClosed = isClosed;
	}

	public void set(VLongWritable ship_ID, VLongWritable iMO,
			Text vesselName, VLongWritable zoneAxsmarine_ID, Text zone_Name,
			VLongWritable entryTime, VLongWritable lastTime,
			BooleanWritable isClosed)
	{
		Ship_ID = ship_ID;
		IMO = iMO;
		VesselName = vesselName;
		ZoneAxsmarine_ID = zoneAxsmarine_ID;
		Zone_Name = zone_Name;
		EntryTime = entryTime;
		LastTime = lastTime;
		IsClosed = isClosed;		
	}
	public VLongWritable getShip_ID() {
		return Ship_ID;
	}

	public void setShip_ID(VLongWritable ship_ID) {
		Ship_ID = ship_ID;
	}

	public VLongWritable getIMO() {
		return IMO;
	}

	public void setIMO(VLongWritable iMO) {
		IMO = iMO;
	}

	public Text getVesselName() {
		return VesselName;
	}

	public void setVesselName(Text vesselName) {
		VesselName = vesselName;
	}

	public VLongWritable getZoneAxsmarine_ID() {
		return ZoneAxsmarine_ID;
	}

	public void setZoneAxsmarine_ID(VLongWritable zoneAxsmarine_ID) {
		ZoneAxsmarine_ID = zoneAxsmarine_ID;
	}

	public Text getZone_Name() {
		return Zone_Name;
	}

	public void setZone_Name(Text zone_Name) {
		Zone_Name = zone_Name;
	}

	public VLongWritable getEntryTime() {
		return EntryTime;
	}

	public void setEntryTime(VLongWritable entryTime) {
		EntryTime = entryTime;
	}

	public VLongWritable getLastTime() {
		return LastTime;
	}

	public void setLastTime(VLongWritable lastTime) {
		LastTime = lastTime;
	}

	public BooleanWritable getIsClosed() {
		return IsClosed;
	}

	public void setIsClosed(BooleanWritable isClosed) {
		IsClosed = isClosed;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		Ship_ID.readFields(in);
		IMO.readFields(in);
		VesselName.readFields(in);
		ZoneAxsmarine_ID.readFields(in);
		Zone_Name.readFields(in);
		EntryTime.readFields(in);
		LastTime.readFields(in);
		IsClosed.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		Ship_ID.write(out);
		IMO.write(out);
		VesselName.write(out);
		ZoneAxsmarine_ID.write(out);
		Zone_Name.write(out);
		EntryTime.write(out);
		LastTime.write(out);
		IsClosed.write(out);
	}

}
