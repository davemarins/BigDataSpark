package lab8ToDo;

import java.io.Serializable;

public class criticalStation implements Serializable {

    private int station, hour;
    private String weekDay;
    private double critical;

    public criticalStation(int station, String weekDay, int hour, double critical) {
        this.station = station;
        this.weekDay = weekDay;
        this.hour = hour;
        this.critical = critical;
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

    public String getWeekDay() {
        return weekDay;
    }

    public void setWeekDay(String weekDay) {
        this.weekDay = weekDay;
    }

    public double getCritical() {
        return critical;
    }

    public void setCritical(double critical) {
        this.critical = critical;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof criticalStation)) return false;

        criticalStation that = (criticalStation) o;

        if (getStation() != that.getStation()) return false;
        if (getHour() != that.getHour()) return false;
        if (Double.compare(that.getCritical(), getCritical()) != 0) return false;
        return getWeekDay().equals(that.getWeekDay());
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = getStation();
        result = 31 * result + getHour();
        result = 31 * result + getWeekDay().hashCode();
        temp = Double.doubleToLongBits(getCritical());
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "criticalStation{" +
                "station=" + station +
                ", hour=" + hour +
                ", weekDay='" + weekDay + '\'' +
                ", critical=" + critical +
                '}';
    }

}
