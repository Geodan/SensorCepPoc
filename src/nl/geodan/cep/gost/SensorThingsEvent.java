package nl.geodan.cep.gost;

/**
 * GOST/SensorThings Event. Only value and topic now.
 * @author 
 *
 */
public class SensorThingsEvent {
    private double value;
    private String topic;
    
    public SensorThingsEvent(String topic, double value) {
        this.value = value;
        this.topic = topic;
    }

    public double getValue() {
        return value;
    }
    public String getTopic() {
    	return topic;
    }
}
