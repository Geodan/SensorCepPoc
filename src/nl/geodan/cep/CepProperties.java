package nl.geodan.cep;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;


public class CepProperties {

	private String PROPERTIES_FILE = "cep.properties";
	Properties properties = new Properties();
	
	private static CepProperties cepproperties = null;
	public static CepProperties getInstance() {
		if (cepproperties==null) {
			cepproperties = new CepProperties();
		}
		return cepproperties;
	}
	
	private CepProperties() {
		 InputStream is = null;
		 
		 try {
		        File f = new File(PROPERTIES_FILE);
		        is = new FileInputStream( f );
		    }
		    catch ( Exception e ) { is = null; }
		 
		    try {
		        if ( is == null ) {
		            //if not found in current directory, try loading it from the class path.
		            is = getClass().getResourceAsStream(PROPERTIES_FILE);
		        }
		        
		        // Try loading properties from the file (if found)
		        properties.load(is);
		        is.close();
		    }
		    catch ( Exception e ) {}
	}


	public String getProperty(String key) {
		String s = properties.getProperty(key);
		if (s==null) 
			return null;
		
		return properties.getProperty(key).trim();
	}

	public int getPropertyAsInteger(String key) {
		return Integer.parseInt(properties.getProperty(key).trim());
	}



	
}
