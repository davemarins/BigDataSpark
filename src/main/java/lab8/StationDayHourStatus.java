package lab8;

import java.io.Serializable;

public class StationDayHourStatus implements Serializable {

    private int station, hour, status;
    private String weekDay;

    public StationDayHourStatus() {
    }

    public StationDayHourStatus(int station, String weekDay, int hour, int status) {
        this.station = station;
        this.weekDay = weekDay;
        this.hour = hour;
        this.status = status;
    }

    public int getStation() {
        return station;
    }

    public void setStation(int station) {
        this.station = station;
    }

    public String getWeekDay() {
        return weekDay;
    }

    public void setWeekDay(String weekDay) {
        this.weekDay = weekDay;
    }

    public int getHour() {
        return hour;
    }

    public void setHour(int hour) {
        this.hour = hour;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

}
