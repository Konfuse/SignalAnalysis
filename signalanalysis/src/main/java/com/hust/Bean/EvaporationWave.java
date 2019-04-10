package com.hust.Bean;

/**
 * @Author: Konfuse
 * @Date: 2019/4/10 上午13:16
 */
public class EvaporationWave {
    private int year;
    private int month;
    private int day;
    private int hour;
    private double lon;
    private double lat;
    private double bdgd;
    private double bdqd;

    public EvaporationWave() {
    }

    public EvaporationWave(int year, int month, int day, int hour, double lon, double lat, double bdgd, double bdqd) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.lon = lon;
        this.lat = lat;
        this.bdgd = bdgd;
        this.bdqd = bdqd;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getHour() {
        return hour;
    }

    public void setHour(int hour) {
        this.hour = hour;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getBdgd() {
        return bdgd;
    }

    public void setBdgd(double bdgd) {
        this.bdgd = bdgd;
    }

    public double getBdqd() {
        return bdqd;
    }

    public void setBdqd(double bdqd) {
        this.bdqd = bdqd;
    }

    @Override
    public String toString() {
        return "EvaporationWave{" +
                "year=" + year +
                ", month=" + month +
                ", day=" + day +
                ", hour=" + hour +
                ", lon=" + lon +
                ", lat=" + lat +
                ", bdgd=" + bdgd +
                ", bdqd=" + bdqd +
                '}';
    }
}
