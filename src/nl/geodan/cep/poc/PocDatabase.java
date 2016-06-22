package nl.geodan.cep.poc;

import nl.geodan.cep.db.CepDatabase;
import nl.geodan.cep.CepProperties;

public class PocDatabase {

	public static void checkPOCTables() {
		String schema = CepProperties.getInstance().getProperty("database.schema");
		
		if (!CepDatabase.checkSchema (schema)) {
			CepDatabase.createSchema (schema);
		}
		
		if (!CepDatabase.checkTable(schema, "routes")) {
			String sql_create = "CREATE TABLE "+schema+ ".routes (id serial PRIMARY KEY, name varchar(60));";
			String sql_addgeometry = "SELECT AddGeometryColumn ('"+schema+ "','routes','geom',4326,'GEOMETRY',2);";
			
			CepDatabase.execute(sql_create);
			CepDatabase.execute(sql_addgeometry);
			FillRoutes(schema);
		}
		
		if (!CepDatabase.checkTable(schema, "routekwaliteit")) {
			String sql_create = "CREATE TABLE "+schema+".routekwaliteit (id serial PRIMARY KEY, routeid integer, luchtkwaliteit integer, verkeer integer, tijd timestamp with time zone);";
			
			sql_create = sql_create.replace("$schema" , schema);
			
			CepDatabase.execute(sql_create);
			CepDatabase.createIndex (schema, "routekwaliteit", "routeid");
			CepDatabase.createIndex (schema, "routekwaliteit", "tijd");
		}
		
		if (!CepDatabase.checkTable(schema, "parkeergarages")) {
			String sql_create = "CREATE TABLE " + schema + ".parkeergarages (id serial PRIMARY KEY, name varchar(60), capaciteit integer);";
			String sql_addgeometry = "SELECT AddGeometryColumn ('" + schema + "','parkeergarages','geom',4326,'POINT',2);";
			
			CepDatabase.execute(sql_create);
			CepDatabase.execute(sql_addgeometry);
			FillGarages(schema);
		}
		
		if (!CepDatabase.checkTable(schema, "parkeergaragestatus")) {
			String sql_create = "CREATE TABLE "+ schema + ".parkeergaragestatus (id serial PRIMARY KEY, parkeergarageid integer, bezetting integer, trend integer, tijd timestamp with time zone);";
			
			CepDatabase.execute(sql_create);
			CepDatabase.createIndex (schema, "parkeergaragestatus", "parkeergarageid");
			CepDatabase.createIndex (schema, "parkeergaragestatus", "tijd");
		}
		
		if (!CepDatabase.checkTable(schema, "view_routes")) {
			String sql = "create view " + schema + ".view_routes as select b.routeid, a.name, a.geom, b.luchtkwaliteit, b.verkeer, b.tijd from " + schema + ".routes a, " + schema + ".routekwaliteit b where a.id = b.routeid order by b.tijd desc limit 1;";

			CepDatabase.execute(sql);
		}
		
		if (!CepDatabase.checkTable(schema, "view_parkeergarages")) {
			String sql = "create view " + schema + ".view_parkeergarages as select b.parkeergarageid, a.name, a.geom, a.capaciteit, b.bezetting, b.trend, b.tijd from " + schema + ".parkeergarages a, " + schema + ".parkeergaragestatus b where a.id = b.parkeergarageid order by b.tijd desc limit 1;";
			
			CepDatabase.execute(sql);
		}
		
		
	}



	private static void FillRoutes(String schema) {
		// TODO Auto-generated method stub
		String sql= "INSERT INTO " + schema + ".routes (name, geom) VALUES ( 'Oudeweg', ST_GeomFromText('POINT(0 0)', 4326) )";
		CepDatabase.execute(sql);
		
		sql= "INSERT INTO " + schema + ".routes (name, geom) VALUES ( 'Schipholweg', ST_GeomFromText('POINT(0 0)', 4326) )";
		CepDatabase.execute(sql);
	}
	
	private static void FillGarages(String schema) {
		// TODO Auto-generated method stub
		String sql= "INSERT INTO " + schema + ".parkeergarages (name, geom, capaciteit) VALUES ( 'Cronjé', ST_GeomFromText('POINT(52.39445097904283 4.6385621664143075)', 4326), 433)";
		CepDatabase.execute(sql);
		
		sql= "INSERT INTO " + schema + ".parkeergarages (name, geom, capaciteit) VALUES ( 'Station', ST_GeomFromText('POINT(52.38660409949515 4.638791338416492)', 4326), 460 )";
		CepDatabase.execute(sql);
		
		sql= "INSERT INTO " + schema + ".parkeergarages (name, geom, capaciteit) VALUES ( 'De Appelaar', ST_GeomFromText('POINT(52.38037815632764 4.638934349710341)', 4326), 296 )";
		CepDatabase.execute(sql);
		
		sql= "INSERT INTO " + schema + ".parkeergarages (name, geom, capaciteit) VALUES ( 'De Kamp', ST_GeomFromText('POINT(52.37727733577011 4.636889297261829)', 4326), 430 )";
		CepDatabase.execute(sql);
		
		sql= "INSERT INTO " + schema + ".parkeergarages (name, geom, capaciteit) VALUES ( 'Raaks', ST_GeomFromText('POINT(52.381488290016314 4.629215089566064)', 4326), 975 )";
		CepDatabase.execute(sql);
		
		sql= "INSERT INTO " + schema + ".parkeergarages (name, geom, capaciteit) VALUES ( 'Houtplein', ST_GeomFromText('POINT(52.37468871937997 4.631665095084625)', 4326), 750 )";
		CepDatabase.execute(sql);
		
		sql= "INSERT INTO " + schema + ".parkeergarages (name, geom, capaciteit) VALUES ( 'Dreef', ST_GeomFromText('POINT(52.37203730645768 4.6304085660569445)', 4326), 116 )";
		CepDatabase.execute(sql);
	}
		
	
}
