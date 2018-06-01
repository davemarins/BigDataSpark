package lab8ToDo;

public class Result {

    private int station, hour;
    private String weekDay;
    private double longitude, latitude, critical;

    public Result(int station, String weekDay, int hour, double longitude, double latitude, double critical) {
        this.station = station;
        this.weekDay = weekDay;
        this.hour = hour;
        this.longitude = longitude;
        this.latitude = latitude;
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

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getCritical() {
        return critical;
    }

    public void setCritical(double critical) {
        this.critical = critical;
    }

}
