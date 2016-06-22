package nl.geodan.cep.poc.parkeer;

/**
 * Event to send dummy data for POC
 * @author 
 *
 */
public class ParkeerEvent {
    private int id;
    private int bezetting;
    
    public ParkeerEvent(int id, int bezetting) {
        this.id = id;
        this.bezetting = bezetting;
    }

    public int getId() {
        return id;
    }
    public int getBezetting() {
    	return bezetting;
    }
}
