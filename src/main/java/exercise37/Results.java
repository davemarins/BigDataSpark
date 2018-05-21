package exercise37;

import java.io.Serializable;

public class Results implements Serializable {

    private String sensorId;
    private double maxPM10;

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public double getMaxPM10() {
        return maxPM10;
    }

    public void setMaxPM10(double maxPM10) {
        this.maxPM10 = maxPM10;
    }

}
