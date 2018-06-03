package lab8;

import java.io.Serializable;
import java.sql.Timestamp;

public class Reading implements Serializable {

    private int station, used_slots, free_slots;
    private Timestamp timestamp;

    public int getStation() {
        return station;
    }

    public void setStation(int station) {
        this.station = station;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
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

}
