package nl.geodan.cep.gost;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

/**
 * 
 * Simple listener example
 * It will receive the aggregated sensordata as defined in the expression
 * TODO: store in database (and check needed data)
 *
 */
public class GostListener implements UpdateListener {
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        EventBean event = newEvents[0];
        
        System.out.println("avg=" + event.get("avg(value)"));
        System.out.println("stddev=" + event.get("stddev(value)"));
    }
}