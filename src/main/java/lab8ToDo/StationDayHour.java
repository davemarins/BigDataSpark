package lab8ToDo;

import java.io.Serializable;

public class StationDayHour implements Serializable {

    private int station, hour, status;
    private String weekDay;

    public StationDayHour(int station, String weekDay, int hour, int status) {
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

    public String getWeekDay() {
        return weekDay;
    }

    public void setWeekDay(String weekDay) {
        this.weekDay = weekDay;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StationDayHour)) return false;

        StationDayHour that = (StationDayHour) o;

        if (getStation() != that.getStation()) return false;
        if (getHour() != that.getHour()) return false;
        if (getStatus() != that.getStatus()) return false;
        return getWeekDay().equals(that.getWeekDay());
    }

    @Override
    public int hashCode() {
        int result = getStation();
        result = 31 * result + getHour();
        result = 31 * result + getStatus();
        result = 31 * result + getWeekDay().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "StationDayHour{" +
                "station=" + station +
                ", hour=" + hour +
                ", status=" + status +
                ", weekDay='" + weekDay + '\'' +
                '}';
    }

}
