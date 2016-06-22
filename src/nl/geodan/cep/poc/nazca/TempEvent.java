package nl.geodan.cep.poc.nazca;

/**
 * Test event from Nazca-I broker
 * @author
 *
 */
public class TempEvent {
    private double temperature;

    public TempEvent(double temperature) {
        this.temperature = temperature;
    }

    public double getTemperature() {
        return temperature;
    }
}
