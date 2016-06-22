package nl.geodan.cep.poc.nazca;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.espertech.esper.client.EPServiceProvider;

import nl.geodan.cep.db.PostgresqlPool;

/**
 * Test client to connect to Nazca-I broker
 * @author 
 *
 */

public class NazcaMqttClient implements MqttCallback {
	EPServiceProvider epService;
	
	MqttClient myClient;
	MqttConnectOptions connOpt;

	static final String BROKER_URL = "*********";
	static final String M2MIO_THING = "haarlemtest3";
	static final String M2MIO_USERNAME = "********";
	static final String M2MIO_PASSWORD_MD5 = "*********";

	// the following two flags control whether this example is a publisher, a subscriber or both
	static final Boolean subscriber = true;

	public NazcaMqttClient(EPServiceProvider epService) {
		this.epService = epService;
		
		Connection con = PostgresqlPool.getInstance().getConnection();
		try {
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
		
	}

	/**
	 * 
	 * connectionLost
	 * This callback is invoked upon losing the MQTT connection.
	 * 
	 */
	@Override
	public void connectionLost(Throwable t) {
		System.out.println("Connection lost!");
		t.printStackTrace();
		// code to reconnect to the broker would go here if desired
		
		runClient();
	}

	/**
	 * 
	 * deliveryComplete
	 * This callback is invoked when a message published by this client
	 * is successfully received by the broker.
	 * 
	 */
	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		//System.out.println("Pub complete" + new String(token.getMessage().getPayload()));
	}

	/**
	 * 
	 * messageArrived
	 * This callback is invoked when a message is received on a subscribed topic.
	 * 
	 */
	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		System.out.println("-------------------------------------------------");
		System.out.println("| Topic:" + topic);
		System.out.println("| Message: " + new String(message.getPayload()));
		System.out.println("-------------------------------------------------");
		
		//parse Nasza Message
		try {
			 JSONParser parser = new JSONParser();
			 Object obj = parser.parse(new String(message.getPayload()));
			 JSONObject jsonobject = (JSONObject)obj;
			 
			 String id = (String)jsonobject.get("id");
			 String name = (String)jsonobject.get("name");
			 String type = (String)jsonobject.get("type");
			 String svalue = (String)jsonobject.get("value");
			 String unit = (String)jsonobject.get("unit");
			 String sdate = (String)jsonobject.get("datetime");
			 
			 Double value = Double.valueOf(svalue);
			 //long datetime = Date.parse(sdate);
			 
			 SaveRecord (id, name, type, value, unit, sdate);
			 //long t = (long)jsonobject.get("result");
			 //System.out.println(t);
			 
			 if (type.equalsIgnoreCase("TCA")) {
				 TempEvent event = new TempEvent(value);
				 epService.getEPRuntime().sendEvent(event);
				 System.out.println("TempEvent sent");
			 }
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}	
		

	/**
	 * 
	 * MAIN
	 * 
	 */
	/*
	public static void main(String[] args) {
		SimpleMqttClient smc = new SimpleMqttClient();
		smc.runClient();
	}
	*/
	
	private void SaveRecord(String id, String name, String type, Double value, String unit, String date) {
		// TODO Auto-generated method stub
		
		Connection con = null;
		Statement stmt = null;
		try {
			con = PostgresqlPool.getInstance().getConnection();
			stmt = con.createStatement();
		    // use connection
			String sql = "INSERT INTO haarlem.sensors (ID,NAME,TYPE,VALUE,UNIT,DATETIME) "
		               + "VALUES ('" + id +  "', '" + name +  "', '" + type +  "', " + value +  ", '" + unit +  "', '" + date +  "' );";
		    System.out.println(sql);
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

	/**
	 * 
	 * runClient
	 * The main functionality of this simple example.
	 * Create a MQTT client, connect to broker, pub/sub, disconnect.
	 * 
	 */
	public void runClient() {
		// setup MQTT Client
		String clientID = M2MIO_THING;
		connOpt = new MqttConnectOptions();
		
		connOpt.setCleanSession(true);
		connOpt.setKeepAliveInterval(30);
		connOpt.setUserName(M2MIO_USERNAME);
		connOpt.setPassword(M2MIO_PASSWORD_MD5.toCharArray());
		
		
		// Connect to Broker
		try {
			myClient = new MqttClient(BROKER_URL, clientID);
			myClient.setCallback(this);
			myClient.connect(connOpt);
		} catch (MqttException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
		System.out.println("Connected to " + BROKER_URL);

		String myTopic = "haarlem/sensoren/#";

		// subscribe to topic if subscriber
		if (subscriber) {
			try {
				int subQoS = 0;
				myClient.subscribe(myTopic, subQoS);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		
		// disconnect
		/*
		try {
			// wait to ensure subscribed messages are delivered
			if (subscriber) {
				Thread.sleep(5000);
			}
			myClient.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		*/
	}




}