package lab8;

import java.io.Serializable;

public class StationDayHourCritical implements Serializable {

    private int station, hour;
    private String weekDay;
    private double critical;

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

    public double getCritical() {
        return critical;
    }

    public void setCritical(double critical) {
        this.critical = critical;
    }

}
