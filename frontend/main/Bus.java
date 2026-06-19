package com.example.mywidget;

public class Bus {
    public String number;
    public String fromStation;
    public String fromTime;
    public String toStation;
    public String toTime;
    public String totalTime;
    public String status;
    public String trackId;
    private boolean isSubscribed = false;

    public Bus(String number, String fromStation, String fromTime,
               String toStation, String toTime, String totalTime) {
        this.number = number;
        this.fromStation = fromStation;
        this.fromTime = fromTime;
        this.toStation = toStation;
        this.toTime = toTime;
        this.totalTime = totalTime;
        this.status = "active";
    }

    public boolean hasValidTime() {
        return totalTime != null;
    }

    public int getTimeInMinutes() {
        try {
            return Integer.parseInt(totalTime.replace(" мин", ""));
        } catch (Exception e) {
            return Integer.MAX_VALUE;
        }
    }

    public boolean isSubscribed() {
        return isSubscribed;
    }

    public void setSubscribed(boolean subscribed) {
        isSubscribed = subscribed;
    }
}