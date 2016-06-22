package nl.geodan.cep.gost;


import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import com.espertech.esper.client.EPServiceProvider;

import nl.geodan.cep.CepProperties;
import nl.geodan.cep.db.CepDatabase;

public class GostMqttClient implements MqttCallback {
	EPServiceProvider epService;
	
	
	MqttClient mqttclient;
	MqttConnectOptions connOpt;
	
	//TODO: put these in a class, quite ugly right now
	String[] topics;
	String[] types;
	int[] qoss;
	int[] stores;
	String [] sensors;
	boolean bstoring = false;

	
	public GostMqttClient(EPServiceProvider epService) {
		this.epService = epService;
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

		//try to reconnect
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
		
	}

	/**
	 * 
	 * messageArrived
	 * This callback is invoked when a message is received on a subscribed topic.
	 * 
	 */
	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		
		//GOST message looks like:
		/*
			Topic:Datastreams(9)/Observations
			Message: {"@iot.id":2115,"@iot.selfLink":"http://gost.geodan.nl/v1.0/Observations(2115)","phenomenonTime":"2016-06-10T12:38:19Z","result":24.52,"Datastream@iot.navigationLink":"http://gost.geodan.nl/v1.0/Observations(2115)/Datastream","FeatureOfInterest@iot.navigationLink":"http://gost.geodan.nl/v1.0/Observations(2115)/FeatureOfInterest"}
		 */
		
		//parse GostMessage
		try {
			 JSONParser parser = new JSONParser();
			 Object obj = parser.parse(new String(message.getPayload()));
			 JSONObject jsonobject = (JSONObject)obj;
			 
			 Object o = jsonobject.get("result");
			 long iotid = (long)jsonobject.get("@iot.od");
			 String sdate = (String)jsonobject.get("phenomenonTime");
			 
			 if (o != null) {
				 double value;
				 if(o instanceof Long) 
			           value = ((Long)o).doubleValue();
				 else
					 	value = (double)o;
				 
				 
				 
				 //Send event to Esper
				 SensorThingsEvent event = new SensorThingsEvent(topic,value);
				 epService.getEPRuntime().sendEvent(event);
				 
				 
				 //check if it needs to be saved
				 //to be replaced with class/collection
				 if (hasToBeSaved (topic) ) {
					 String sensorid = getSensorId (topic);
					 String type = getSensorType (topic);
					 CepDatabase.saveSensorRecord(sensorid, iotid, type, value , sdate);
				 }
				 
			 }
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}	
	

	//TODO: to be replaced with class/collection
	private boolean hasToBeSaved (String topic) {
		for (int i=0; i<topics.length;i++)
			if (topics[i].equalsIgnoreCase(topic) && stores[i]>0) 
				return true;
		return false;
	}
	
	//TODO to be replaced with class/collection
	private String getSensorId (String topic) {
		for (int i=0; i<topics.length;i++)
			if (topics[i].equalsIgnoreCase(topic)) 
				return sensors[i];
		return null;
	}
	
	//TODO to be replaced with class/collection
	private String getSensorType (String topic) {
		for (int i=0; i<topics.length;i++)
			if (topics[i].equalsIgnoreCase(topic)) 
				return types[i];
		return null;
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
		String broker_url = CepProperties.getInstance().getProperty("mqtt.server");
		String clientID = CepProperties.getInstance().getProperty("mqtt.thing");
		
		connOpt = new MqttConnectOptions();
		connOpt.setCleanSession(true);
		connOpt.setKeepAliveInterval(30);
		
		String username = CepProperties.getInstance().getProperty("mqtt.username");
		String password = CepProperties.getInstance().getProperty("mqtt.password");
		
		if (username!=null && password!=null) {
			connOpt.setUserName(username);
			connOpt.setPassword(password.toCharArray());
		}
		
		
		// Connect to Broker
		try {
			mqttclient = new MqttClient(broker_url, clientID);
			mqttclient.setCallback(this);
			mqttclient.connect(connOpt);
		} catch (MqttException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
		System.out.println("Connected to " + broker_url);

		//get data about topics to subscribe to from property file
		int nrtopics = CepProperties.getInstance().getPropertyAsInteger("mqtt.topics.count");
		
		//TODO: put these in a class
		topics = new String[nrtopics];
		types = new String[nrtopics];
		qoss = new int[nrtopics];
		stores = new int[nrtopics];
		sensors = new String[nrtopics];
		
		boolean bstoring = false;
		
		for (int i=0; i<nrtopics; i++) {
			topics[i] = CepProperties.getInstance().getProperty("mqtt.topics." + i + ".topic");
			qoss[i] = CepProperties.getInstance().getPropertyAsInteger("mqtt.topics." + i + ".qos");
			stores[i] = CepProperties.getInstance().getPropertyAsInteger("mqtt.topics." + i + ".store");
			types[i] = CepProperties.getInstance().getProperty("mqtt.topics." + i + ".type");
			sensors[i] = CepProperties.getInstance().getProperty("mqtt.topics." + i + ".sensor");
			if (stores[i]>0) bstoring = true;
		}
		
		//are sensor values to be stored in the database?
		if (bstoring) {
			CepDatabase.checkGOSTTable();
			for (int i=0; i<topics.length; i++)
			{
				CepDatabase.checkSensorView (sensors[i], types[i]);
			}
		}

		// subscribe to topics as defined in property file
		try {
			mqttclient.subscribe(topics, qoss);
			
			System.out.println("Subscribed to: " );
			for (int i=0; i<topics.length;i++) 
				System.out.println ("    " + topics[i] + " |qos:" + qoss[i]);
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}