package com.ian.mooc.data.flink;

import java.sql.Timestamp;

public class Action {
    public String actionType;
    public String timeStamp;
    public int userId;
    public String courseId;
    public String sessionId;

    public Action() {
    }

    public Action(String actionType, String timeStamp, int userId, String courseId, String sessionId) {
        this.actionType = actionType;
        this.timeStamp = timeStamp;
        this.userId = userId;
        this.courseId = courseId;
        this.sessionId = sessionId;
    }

    @Override
    public String toString() {
        return "Action{" +
                "actionType='" + actionType + '\'' +
                ", timeStamp='" + timeStamp + '\'' +
                ", userId=" + userId +
                ", courseId='" + courseId + '\'' +
                ", sessionId='" + sessionId + '\'' +
                '}';
    }
}
