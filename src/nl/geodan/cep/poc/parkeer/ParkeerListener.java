package nl.geodan.cep.poc.parkeer;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

/**
 * Esper Listener retrieving information about Parkeergarages.
 * TODO: store in dummydata database  
 * @author
 *
 */
public class ParkeerListener implements UpdateListener {
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        EventBean event = newEvents[0];
        
        String s = "id[" + event.get("id") + "] " + event.get("avg(bezetting)") + " " +event.get("first(bezetting)") + " " + event.get("last(bezetting)");
        
        System.out.println (s);
        
        /*
        System.out.println("id=" + event.get("id"));
        System.out.println("avg=" + event.get("avg(bezetting)"));
        System.out.println("first=" + event.get("first(bezetting)"));
        System.out.println("last=" + event.get("last(bezetting)"));
        //System.out.println("stddev=" + event.get("stddev(value)"));
         
         */
    }
}