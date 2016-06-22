package nl.geodan.cep;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

import nl.geodan.cep.db.CepDatabase;
import nl.geodan.cep.gost.GostListener;
import nl.geodan.cep.gost.GostMqttClient;
import nl.geodan.cep.poc.PocDatabase;
import nl.geodan.cep.poc.parkeer.ParkeerEvent;
import nl.geodan.cep.poc.parkeer.ParkeerListener;



public class CepPocMain {

	
	
	public static void main(String[] args) {
		//check if		
		CepDatabase.check();
		PocDatabase.checkPOCTables();
		
		
		EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider();
		
		/* GOST listener */
		// example of listening to specific datastream. 
		//TODO: define expressions in properties file.
		String expression = "select avg(value), stddev(value) from nl.geodan.cep.gost.SensorThingsEvent(topic=\"Datastreams(9)/Observations\").win:time(120 sec) output snapshot every 30 seconds";
		EPStatement statement = epService.getEPAdministrator().createEPL(expression);		
		GostListener listener = new GostListener();
		statement.addListener(listener);
		
		/* Parkeergaragelistener */
		for (int i=0; i<7; i++)
		{
			//String expressionp = "select id, avg(bezetting), first(bezetting), last(bezetting)  from nl.geodan.cep.gostesper.ParkeerEvent(id="+ i + ").win:time(60 sec) output snapshot every 15 seconds";
			
			// give the average of the last 15 minutes, every 5 minutes. 
			// The last and first are used to calculate the trend. 
			// last is also used for current 'bezetting'.
			String expressionp = "select id, avg(bezetting), first(bezetting), last(bezetting)  from nl.geodan.cep.poc.parkeer.ParkeerEvent(id="+ i + ").win:time(15 minutes) output snapshot every 5 minutes";
			EPStatement statementp = epService.getEPAdministrator().createEPL(expressionp);
			
			ParkeerListener listenerp = new ParkeerListener();
			statementp.addListener(listenerp);
		}
		
		/* Start the GOST Mqtt client. */
		// This client will subscribe to GOST and get sensordata.
		// the sensordata is put into a GostEVent by the GostMqttClient
		try {
			GostMqttClient smc = new GostMqttClient(epService);
			smc.runClient();
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		
		
		// Test code to send ParkeerEvents with random data
		Runnable runCurrent = new Runnable() {
		    public void run() {
		    	EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider();
		    	for (int pi=0; pi<7; pi++) { 
			    	int bezetting = (int )(Math.random() * 50 + 1);
					
					ParkeerEvent event = new ParkeerEvent(pi, bezetting);
					epService.getEPRuntime().sendEvent(event);
					System.out.println ("bezetting [id=" + pi + "]: " + bezetting);
		    	}
				
		    }
		};
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		executor.scheduleAtFixedRate(runCurrent, 0, 10, TimeUnit.SECONDS); 
		
		

		
		
		
	}
	
	
}
