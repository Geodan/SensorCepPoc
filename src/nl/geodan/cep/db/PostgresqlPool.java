package nl.geodan.cep.db;

import java.sql.*; 
import javax.sql.*;

import org.postgresql.jdbc3.Jdbc3PoolingDataSource;

import nl.geodan.cep.CepProperties; 

/**
 * Class 
 * @author 
 *
 */
public class PostgresqlPool {
	
	static private PostgresqlPool instance;
	
	static public PostgresqlPool getInstance() {
		if (instance==null)
			instance = new PostgresqlPool();
		return instance;
	}
	
	private Jdbc3PoolingDataSource datasource ;

	private PostgresqlPool () {
		
		datasource = new Jdbc3PoolingDataSource();
		
		datasource.setDataSourceName (CepProperties.getInstance().getProperty("database.datasource"));
		datasource.setServerName (CepProperties.getInstance().getProperty("database.server"));
		datasource.setDatabaseName(CepProperties.getInstance().getProperty("database.database"));
		datasource.setUser(CepProperties.getInstance().getProperty("database.user"));
		datasource.setPassword(CepProperties.getInstance().getProperty("database.password"));
		datasource.setMaxConnections(CepProperties.getInstance().getPropertyAsInteger("database.pooling.maxconnections"));

		
		//TODO: move code below to CepDatabase.
		Connection con = null;
		try {
			con = datasource.getConnection();
		    
			Statement st = con.createStatement();
	        String qs = "CREATE TABLE IF NOT EXISTS haarlem.sensors(oid SERIAL NOT NULL PRIMARY KEY,id varchar(225) NOT NULL, name varchar(225), type varchar(10), value double precision, unit varchar(10), datetime timestamp)";
	         st.execute(qs);			
			
		} catch (SQLException e) {
		    // log error
			e.printStackTrace();
		} finally {
		    if (con != null) {
		        try { con.close(); } catch (SQLException e) {}
		    }
		    
		}
		
		
		
		
	}
	
	/**
	 * Retrieve connection from datapool. Do not forget to close it!!!
	 * @return connection
	 */
	public Connection getConnection() {
		Connection con = null;
		try {
		    con = datasource.getConnection();
		    return con;
		    // use connection
		} catch (SQLException e) {
		    // log error
			return null;
		} 
	}

}
