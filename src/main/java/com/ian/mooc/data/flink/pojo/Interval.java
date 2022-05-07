package com.ian.mooc.data.flink.pojo;

import java.util.Objects;

public class Interval {
    private Long intervalValue;
    private String intervalType;
    private Integer user;
    private String course;
    private String session;
    public Interval(Long intervalValue, String intervalType, Integer user, String course, String session){
        this.intervalValue = intervalValue;
        this.intervalType = intervalType;
        this.user = user;
        this.course = course;
        this.session = session;
    }

    public Long getIntervalValue() {
        return intervalValue;
    }

    public void setIntervalValue(Long intervalValue) {
        this.intervalValue = intervalValue;
    }

    public String getIntervalType() {
        return intervalType;
    }

    public void setIntervalType(String intervalType) {
        this.intervalType = intervalType;
    }

    public Integer getUser() {
        return user;
    }

    public void setUser(Integer user) {
        this.user = user;
    }

    public String getCourse() {
        return course;
    }

    public void setCourse(String course) {
        this.course = course;
    }

    public String getSession() {
        return session;
    }

    public void setSession(String session) {
        this.session = session;
    }


    @Override
    public String toString() {
        return "Interval{" +
                "intervalValue=" + intervalValue +
                ", intervalType='" + intervalType + '\'' +
                ", user=" + user +
                ", course='" + course + '\'' +
                ", session='" + session + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Interval interval = (Interval) o;
        return Objects.equals(getIntervalValue(), interval.getIntervalValue()) && Objects.equals(getIntervalType(), interval.getIntervalType()) && Objects.equals(getUser(), interval.getUser()) && Objects.equals(getCourse(), interval.getCourse()) && Objects.equals(getSession(), interval.getSession());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getIntervalValue(), getIntervalType(), getUser(), getCourse(), getSession());
    }
}
