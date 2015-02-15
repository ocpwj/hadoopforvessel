package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v6aggregration.eventcalc;

import java.io.File;
import java.io.BufferedReader;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.FileOutputStream;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.StringTokenizer;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import oracle.jdbc.driver.OracleTypes;
import oracle.sql.CLOB;

import org.apache.hadoop.io.Writable;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;

import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.geometry.jts.WKTReader2;

public class VesselZone implements java.io.Serializable {
	
	public static Geometry[] GlobalZones;
	
	static 
	{
		GlobalZones = new Geometry[8];
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
		
		try {
			GlobalZones[0]=createPolygonByWKT(geometryFactory, "POLYGON ((0.0 0.0, 0.0 90.0, -90 90, -90.0 0.0, 0.0 0.0))");
			GlobalZones[1]=createPolygonByWKT(geometryFactory, "POLYGON ((-90.0 0.0, -90.0 90.0, -180.0 90.0, -180.0 0.0, -90.0 0.0))");
			GlobalZones[2]=createPolygonByWKT(geometryFactory, "POLYGON ((0.0 0.0, 90.0 0.0, 90.0 90.0, 0 90, 0.0 0.0))");
			GlobalZones[3]=createPolygonByWKT(geometryFactory, "POLYGON ((90.0 0.0, 180.0 0.0, 180.0 90.0, 90.0 90.0, 90.0 0.0))");
			GlobalZones[4]=createPolygonByWKT(geometryFactory, "POLYGON ((0.0 0.0, -90.0 0.0, -90.0 -90.0, 0 -90, 0.0 0.0))");
			GlobalZones[5]=createPolygonByWKT(geometryFactory, "POLYGON ((-90.0 0.0, -180.0 0.0, -180.0 -90.0, -90.0 -90.0, -90.0 0.0))");
			GlobalZones[6]=createPolygonByWKT(geometryFactory, "POLYGON ((0.0 0.0, 0.0 -90.0, 90 -90, 90.0 0.0, 0.0 0.0))");
			GlobalZones[7]=createPolygonByWKT(geometryFactory, "POLYGON ((90.0 0.0, 90.0 -90.0, 180.0 -90.0, 180.0 0.0, 90.0 0.0))");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static final String GetVesselZones = "select czo_id, axs_id axsmarine_id, zone_name name, zone_type, vzc.axsmarine_name classification_name "
		+"from vessel_zone_v vzo, product_mapping pma, vessel_zone_classification vzc "
		+"where vzo.vzo_id is not null "
		+"and vzo.vzo_id = pma.vzo_id "
		+"and pma.vzc_id = vzc.id "
		+"and exists ( "
		+"select 1 from vessel_zone_geometry t where t.id = vzo.vzo_id and t.polygon.st_isvalid() = 1 ) "
		+"union all "
		+"select czo_id, axs_id axsmarine_id, zone_name name, zone_type, null classification_name "
		+"from vessel_zone_v vzo "
		+"where exists ( "
		+"select 1 from port_geometry t where t.id = vzo.por_id and t.polygon.st_isvalid() = 1 "
		+"union all "
		+"select 1 from anchorage_geometry t where t.id = vzo.anc_id and t.polygon.st_isvalid() = 1 "
		+"union all "
		+"select 1 from berth_geometry t where t.id = vzo.ber_id and t.polygon.st_isvalid() = 1 ) ";
	
	private static final String GetWKT = "{call cef_cnr.kml_util_pkg.get_ves_placemark_info_proc(?, ?, ?, ?)}";

	private final static String Get_Skip_Zone_Types = "select value from application_parm_cfg where application_name='VESSEL_LOAD_PKG' and parameter_name='SKIP_EVENT_TYPES'";
	
	public static HashMap<Integer, VesselZone> ZoneMap = null;

	public int getAxsmarine_ID() {
		return Axsmarine_ID;
	}
	public String getName() {
		return Name;
	}
	public Geometry getPolygon() {
		return Polygon;
	}
	
	public String getZone_type() {
		return Zone_type;
	}	
	
	public boolean IntersectedWithGlobalZone(Integer GZoneIdx)
	{
		return IntersectedGlobalZones.contains(GZoneIdx);
	}
	
	public VesselZone(int axsmarine_id, String name, String zone_type, Geometry polygon, ArrayList<Integer> intersectedglobalzones) {
		super();
		this.Axsmarine_ID = axsmarine_id;
		this.Zone_type=zone_type;
		this.Name = name;
		this.Polygon = polygon;
		this.IntersectedGlobalZones = intersectedglobalzones;
	}
	
	private int Axsmarine_ID;
	private String Name;
	private String Zone_type;
	private Geometry Polygon;
	private ArrayList<String> Zone_Classifications=new ArrayList<String>();
	private ArrayList<Integer> IntersectedGlobalZones = new ArrayList<Integer>();
	
	public void AddClassification(String cf)
	{
		if (!Zone_Classifications.contains(cf))
		{
			Zone_Classifications.add(cf);
		}
	}
	
	public boolean HasClassification(String cf)
	{
		return this.Zone_Classifications.contains(cf);
	}
	////////////////////////////////////////////////////////////////////////
	//overwrite equals() method
	////////////////////////////////////////////////////////////////////////
	public boolean equals(Object x) {
		if (x instanceof VesselZone) {
			if (((VesselZone) x).getAxsmarine_ID() == this.getAxsmarine_ID()) {
				return true;
			}
		}
		return false;
	}

	////////////////////////////////////////////////////////////////////////
	//overwrite hashCode() method
	////////////////////////////////////////////////////////////////////////
	public int hashCode() {
		return this.getAxsmarine_ID();
	}
	
	//////////////////////////////
	//Load Vessel Zones from database
	////////////////////////////
	public static void main(String[] args) throws IOException
	{
		ZoneMap=GetAllZonesFromDB();
		File TempFile=new File("Zones");
		ObjectOutputStream OOS= new ObjectOutputStream(new FileOutputStream(TempFile));
		OOS.writeObject(ZoneMap);
		OOS.close();
		
		System.out.println("Done!");
	}
	
	
	public static HashMap<Integer, VesselZone> GetAllZonesFromDB()
	{
		
		ArrayList<String> Skip_Zone_Type=Get_Skip_Zone_Types();

		Connection DBConn=GetDBConnection();

		try {
			PreparedStatement objPreStatement = DBConn.prepareStatement(GetVesselZones);
			ResultSet objResult = objPreStatement.executeQuery();

			objResult.setFetchSize(200);
			
			GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);			
			HashMap<Integer, VesselZone> Zones = new HashMap<Integer, VesselZone>();	

			while (objResult.next()) {
				
				int AXSMARINE_ID = objResult.getInt("axsmarine_id");
				
				if (!Zones.containsKey(AXSMARINE_ID))
				{
					String zone_type = objResult.getString("zone_type");
					
					////////////////////////
					//check and skip zone types
					if (Skip_Zone_Type.contains(zone_type))
					{
						continue;
					}

					String Name = objResult.getString("name");
					System.out.println("Downloading polygon: "+ AXSMARINE_ID+", "+ Name);
					String Vessel_Classification=objResult.getString("classification_name");

					CallableStatement objStatement=DBConn.prepareCall(GetWKT);
					objStatement.setInt("axsmarine_id_in", AXSMARINE_ID);
					objStatement.registerOutParameter("name_out", OracleTypes.VARCHAR);
					objStatement.registerOutParameter("cur_ves_extended_data_out", OracleTypes.CURSOR);
					objStatement.registerOutParameter("polygon_out", OracleTypes.CLOB);
					objStatement.execute();

					CLOB clob = (CLOB) objStatement.getClob("polygon_out");
					
					BufferedReader br = new BufferedReader(
							clob.getCharacterStream());
					String WKT = "";
					String temp_WKT = br.readLine();
					while (temp_WKT != null) {
						WKT = WKT + temp_WKT;
						temp_WKT = br.readLine();
					}

					ResultSet result_set = (ResultSet) objStatement.getObject("cur_ves_extended_data_out");
					result_set.close();
					
					br.close();
					objStatement.close();

					Geometry polygon=null;
					try {
						polygon = createPolygonByWKT(geometryFactory, WKT);
						
						ArrayList<Integer> IntersectedGlobalZones = new ArrayList<Integer>();
						
						for (int i=0 ; i<GlobalZones.length;i++)
						{
							if (polygon.intersects(GlobalZones[i]))
							{
								IntersectedGlobalZones.add(i);
							}
						}
						
						VesselZone thisZone = new VesselZone(AXSMARINE_ID, Name, zone_type, polygon, IntersectedGlobalZones);
						
						if (Vessel_Classification!=null)
						{
							thisZone.AddClassification(Vessel_Classification);
						}
						
						Zones.put(AXSMARINE_ID, thisZone);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						System.err.println("Can't parse WKT:"+WKT+" for zone:"+AXSMARINE_ID+"--"+Name);
						
						e.printStackTrace();
					}


				}
				else
				{
					VesselZone thisZone=Zones.get(AXSMARINE_ID);
					String Vessel_Classification=objResult.getString("classification_name");
					if (Vessel_Classification!=null)
					{
						thisZone.AddClassification(Vessel_Classification);
					}
					thisZone.AddClassification(Vessel_Classification);
				}
			}

			objPreStatement.close();
			
			return Zones;


		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			try {
				DBConn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return null;
		
	}
	
	private static Geometry createPolygonByWKT(GeometryFactory GF, String WKT)
	throws ParseException {
		WKTReader2 reader = new WKTReader2(GF);
		Geometry polygon = reader.read(WKT);
		return polygon;
	}
	
	private static ArrayList<String> Get_Skip_Zone_Types()
	{
		ArrayList<String> Skip_Zone_Types = new ArrayList<String>();

		Connection DBConn = null;
		try {
			DBConn = GetDBConnection();
			Statement objStatement = DBConn.createStatement();
			ResultSet result_set = objStatement.executeQuery(Get_Skip_Zone_Types);
			
			if (result_set.next()) {
				String skip_types = result_set.getString("value");
				StringTokenizer zone_type_tokenizer = new StringTokenizer(skip_types,";");

				while(zone_type_tokenizer.hasMoreTokens()) {
					String zone_type= zone_type_tokenizer.nextToken();
					Skip_Zone_Types.add(zone_type);
				}
			}
			result_set.close();
			objStatement.close();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				DBConn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return Skip_Zone_Types;
	}
	
	private static Connection GetDBConnection()
	{
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");  
			Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@172.26.66.135:5703:CNR", "cef_cnr", "cef_cnr");
			return conn;
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}

}
