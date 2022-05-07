package com.ian.mooc.data.flink.pojo;

import java.util.Objects;

public class Scene {
    private String sceneType;
    private Integer user;
    private String course;
    private String session;
    public Scene(){}
    public Scene(String sceneType,Integer user,String course,String session){
        this.sceneType = sceneType;
        this.course = course;
        this.session = session;
        this.user = user;
    }
    public void setSession(String session) {
        this.session = session;
    }

    public void setCourse(String course) {this.course = course;}
    public void setSceneType(String sceneType){this.sceneType=sceneType;}
    public void setUser(Integer user) {this.user = user;}

    public String getSession() {
        return session;
    }

    public String getCourse() {
        return course;
    }

    public Integer getUser() {
        return user;
    }

    public String getSceneType() {
        return sceneType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Scene scene = (Scene) o;
        return Objects.equals(getSceneType(), scene.getSceneType()) && Objects.equals(getUser(), scene.getUser()) && Objects.equals(getCourse(), scene.getCourse());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSceneType(), getUser(), getCourse());
    }

    @Override
    public String toString() {
        return "Scene{" +
                "sceneType='" + sceneType + '\'' +
                ", user=" + user +
                ", course='" + course + '\'' +
                ", session='" + session + '\'' +
                '}';
    }
}
