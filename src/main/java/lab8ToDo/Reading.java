package lab8ToDo;

import java.io.Serializable;
import java.sql.Timestamp;

public class Reading implements Serializable {

    private int station, used_slots, free_slots;
    private Timestamp timestamp;

    public Reading(int station, Timestamp timestamp, int used_slots, int free_slots) {
        this.station = station;
        this.timestamp = timestamp;
        this.used_slots = used_slots;
        this.free_slots = free_slots;
    }

    public int getStation() {
        return station;
    }

    public void setStation(int station) {
        this.station = station;
    }

    public int getUsed_slots() {
        return used_slots;
    }

    public void setUsed_slots(int used_slots) {
        this.used_slots = used_slots;
    }

    public int getFree_slots() {
        return free_slots;
    }

    public void setFree_slots(int free_slots) {
        this.free_slots = free_slots;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Reading)) return false;

        Reading reading = (Reading) o;

        if (getStation() != reading.getStation()) return false;
        if (getUsed_slots() != reading.getUsed_slots()) return false;
        if (getFree_slots() != reading.getFree_slots()) return false;
        return getTimestamp().equals(reading.getTimestamp());
    }

    @Override
    public int hashCode() {
        int result = getStation();
        result = 31 * result + getUsed_slots();
        result = 31 * result + getFree_slots();
        result = 31 * result + getTimestamp().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Reading{" +
                "station=" + station +
                ", used_slots=" + used_slots +
                ", free_slots=" + free_slots +
                ", timestamp=" + timestamp +
                '}';
    }

}
