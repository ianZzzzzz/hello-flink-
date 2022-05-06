package com.ian.mooc.data.flink.pojo;

public class Action {
    public String actionType;

    public int user;
    public String course;
    public String session;

    public Action() {
    }

    public Action(String actionType, int userId, String courseId, String sessionId) {
        this.actionType = actionType;

        this.user = userId;
        this.course = courseId;
        this.session = sessionId;
    }
    public void setAction(String actionType, int userId, String courseId, String sessionId) {
        this.actionType = actionType;

        this.user = userId;
        this.course = courseId;
        this.session = sessionId;
    }

    public int getUser() {
        return user;
    }

    public String getCourse() {
        return course;
    }

    public String getActionType() {
        return actionType;
    }

    public String getSession() {
        return session;
    }


    @Override
    public String toString() {
        return "Action{" +
                "actionType='" + actionType + '\'' +
                ", userId=" + user +
                ", courseId='" + course + '\'' +
                ", sessionId='" + session + '\'' +
                '}';
    }
}
