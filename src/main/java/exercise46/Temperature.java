package exercise46;

import java.io.Serializable;

public class Temperature implements Serializable {

    private Integer timestamp;
    private Double temperature;

    public Temperature(Integer timestamp, Double temperature) {
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public Integer getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Integer timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "Temperature{" +
                "timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }

}
