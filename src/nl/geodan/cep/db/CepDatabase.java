package nl.geodan.cep.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import nl.geodan.cep.CepProperties;

public class CepDatabase {
	

	/*
	 * Check if the schema and/or tables needed for storing the aggregated sensordata exist.
	 * If they do not exist, they are created.  
	 * 
	 * The creation of the schemas and tables is not yet implemented.
	 */
	public static void check() {
		
		if (!checkSchema ("haarlem")) {
			System.out.println ("Schema haarlem does not exist or no rights to access database");
		}
		
		//check table routes
		if (!checkTable ("haarlem", "sensors")) {
			System.out.println ("haarlem.sensors does not exist or no rights to access table");
		}
		
		if (!checkTable ("haarlem", "parkeerzones")) {
			System.out.println ("haarlem.parkeerzones does not exist or no rights to access table");
		}
		
	}

	/**
	 * Checks if a table exists in a specified schema.
	 * @param schemaname name of the schema the table should be in
	 * @param tablename name of the table 
	 * @return true or false
	 */
	public static boolean checkTable(String schemaname, String tablename) {
		String sql = "SELECT EXISTS (" + 
				"				   SELECT 1 " + 
				"				   FROM   information_schema.tables " + 
				"				   WHERE  table_schema = '" + schemaname + "' " + 
				"				   AND    table_name = '" + tablename + "' " + 
				"				);";
		

		Connection con = null; 
		
		try {
			con = PostgresqlPool.getInstance().getConnection();
			Statement st = con.createStatement();
			ResultSet rs = st.executeQuery(sql);
			if (rs.next()){
				boolean b = rs.getBoolean(1);
				return b;
			}
			else
				return false;
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (con!=null)
				try {
					con.close();
				} catch (SQLException e) {
					e.printStackTrace();
				} 
		}
		
		
		return false;
		
	}
	
	/** 
	 * Checks if a schema exist in the database
	 * @param schemaname schema to be checked
	 * @return true or false
	 */
	public static boolean checkSchema (String schemaname) {
		String sql = "SELECT exists(select 1 FROM information_schema.schemata WHERE schema_name = '" + schemaname + "');";
		
		Connection con = null; 
		
		try {
			con = PostgresqlPool.getInstance().getConnection();
			Statement st = con.createStatement();
			ResultSet rs = st.executeQuery(sql);
			if (rs.next()){
				boolean b = rs.getBoolean(1);
				return b;
			}
			else
				return false;
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (con!=null)
				try {
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
		}
		
		
		return false;
			
		
	}

	public static void createSchema(String schemaname) {
		String sql = "CREATE SCHEMA $schemaname;";
		
		sql = sql.replace ("$schemaname", schemaname);
		
		Connection con = null; 
		
		try {
			con = PostgresqlPool.getInstance().getConnection();
			Statement st = con.createStatement();
			st.execute(sql);
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (con!=null)
				try {
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
		}
			
	}
	
	public static void execute (String sql) {
		Connection con = null; 
		
		try {
			con = PostgresqlPool.getInstance().getConnection();
			Statement st = con.createStatement();
			st.execute(sql);
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (con!=null)
				try {
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
		}
			
	}

	public static void checkGOSTTable() {
		//| Topic:Datastreams(9)/Observations
		//| Message: {"@iot.id":19846,"@iot.selfLink":"http://gost.geodan.nl/v1.0/Observations(19846)","phenomenonTime":"2016-06-13T13:16:18Z","result":13.23,"Datastream@iot.navigationLink":"http://gost.geodan.nl/v1.0/Observations(19846)/Datastream","FeatureOfInterest@iot.navigationLink":"http://gost.geodan.nl/v1.0/Observations(19846)/FeatureOfInterest"}
		String schema = CepProperties.getInstance().getProperty("database.schema");
		
		if (!CepDatabase.checkSchema (schema)) {
			CepDatabase.createSchema (schema);
		}
		
		if (!CepDatabase.checkTable(schema, "sensordata")) {
			String sql = "CREATE TABLE "+schema +"sensordata (id serial PRIMARY KEY, sensorid integer, iotid bigint, type varchar(10), value double precision, datetime timestamp with time zone);";
			CepDatabase.execute(sql);
		}
		
		if (!CepDatabase.checkTable(schema, "sensors")) {
			String sql = "CREATE TABLE "+schema +".sensors (id integer PRIMARY KEY, name VARCHAR(60));";
			String sql_addgeometry = "SELECT AddGeometryColumn ('"+schema +"','sensors','geom',4326,'POINT',2);";
			
			CepDatabase.execute(sql);
			CepDatabase.execute(sql_addgeometry);
			
			FillSensors(schema);
		}
		
	}
	
	public static void saveSensorRecord(String sensorid, long iotid, String type, double value, String sdate) {
		String schema = CepProperties.getInstance().getProperty("database.schema");
		Connection con = null;
		Statement stmt = null;
		try {
			con = PostgresqlPool.getInstance().getConnection();
			stmt = con.createStatement();
			String sql = "INSERT INTO "+schema+".sensors (sensorid,iotid,type,value,datetime) "
		               + "VALUES (" + sensorid +  ","+ iotid +",'" + type +  "', " + value +  ", '" + sdate +  "' );";
			stmt.executeUpdate(sql);
			stmt.close();
			
		} catch (Exception e) {
		    // log error
			e.printStackTrace();
		} finally {
		    if (con != null) {
		        try { con.close(); } catch (SQLException e) {}
		    }
		    
		}
		
	}

	private static void FillSensors(String schema) {
	
		int nrsensors = CepProperties.getInstance().getPropertyAsInteger("mqtt.topics.count");
		for (int i=0; i<nrsensors; i++) {
			String id = CepProperties.getInstance().getProperty ("mqtt.topics." + i + ".id");
			
			String x =  CepProperties.getInstance().getProperty("mqtt.topics." + i + ".x");
			String y =  CepProperties.getInstance().getProperty("mqtt.topics." + i + ".y");
			String name =  CepProperties.getInstance().getProperty("mqtt.topics." + i + ".name");
			
			String sql= "INSERT INTO " + schema + ".sensors (id, name, geom) VALUES ( '" + id + "','" + name + "', ST_GeomFromText('POINT("+ y + " " + x + ")', 4326))";
			CepDatabase.execute(sql);
			
			//create views
			
		}
		
	}
	
	public static String getViewName (String sensorid, String type) {
		return "view_" + "sensor" + sensorid + "_"+type;
		
	}
	
	public static void checkSensorView (String sensorid, String type) {
		String viewname = getViewName (sensorid, type);
		String schema = CepProperties.getInstance().getProperty("database.schema");
		
		if (!checkTable (schema, viewname)) {
			String sql = "create view "+schema + "." + viewname + " as SELECT a.iotid, a.type,a.value, a.datetime, b.name, b.geom FROM sensors.sensordata a, sensors.sensors b WHERE a.type = '" + type + "' AND b.id = "+sensorid + " AND a.sensorid = b.id ORDER BY a.datetime;";
			CepDatabase.execute(sql);
		}
		
	}
	
	public static void createIndex(String schema, String table, String field) {
		String sql = "CREATE INDEX idx_$table_$field ON $schema.$table ($field);";
		sql = sql.replace("$schema", schema);
		sql = sql.replace("$table", table);
		sql = sql.replace("$field", field);
		CepDatabase.execute(sql);
		
	}

}
