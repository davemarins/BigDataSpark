package exercise38;

import java.io.Serializable;

public class Results implements Serializable {

    private String sensorId;
    private Double count;

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public double getCount() {
        return count;
    }

    public void setCount(double count) {
        this.count = count;
    }

}
