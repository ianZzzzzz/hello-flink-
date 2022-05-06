package com.ian.mooc.data.flink.pojo;

import com.alibaba.fastjson.JSONObject;


import java.sql.Time;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Objects;

public class Event {

    public String actionType;
    public int user;
    public String course;
    public String session;
    public LocalDateTime dateTime ;


    public Event(String str){
        JSONObject jsonObject = JSONObject.parseObject(str);
        int year = jsonObject.getIntValue("year");
        int month = jsonObject.getIntValue("month");
        int day = jsonObject.getIntValue("day");
        int hour = jsonObject.getIntValue("hour");
        int minute = jsonObject.getIntValue("minute");
        int second = jsonObject.getIntValue("second");

        this.actionType = jsonObject.getString("actionType");
        this.user =  jsonObject.getIntValue("user");
        this.course = jsonObject.getString("course");
        this.session = jsonObject.getString("session");
        this.dateTime = LocalDateTime.of(year, month, day, hour, minute, second);

    }

    public int getUser() {
        return user;
    }

    public String getActionType() {
        return actionType;
    }
    public Long getTimestamp(){

        return dateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    @Override
    public String toString() {
        return "Event{" +
                "actionType='" + actionType + '\'' +
                ", user=" + user +
                ", course='" + course + '\'' +
                ", session='" + session + '\'' +
                ", dateTime=" + dateTime +'\''+
                ", timestamp="+ getTimestamp()+
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return getUser() == event.getUser() && Objects.equals(getActionType(), event.getActionType()) && Objects.equals(course, event.course) && Objects.equals(session, event.session) && Objects.equals(dateTime, event.dateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getActionType(), getUser(), course, session, dateTime);
    }
}
