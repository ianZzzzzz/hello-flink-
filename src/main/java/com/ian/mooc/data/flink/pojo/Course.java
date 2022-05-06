package com.ian.mooc.data.flink.pojo;

public class Course {
    public String id;
    public String category;
    public String startTime;

    public Course() {
    }

    public Course(String id, String category, String startTime) {
        this.id = id;
        this.category = category;
        this.startTime = startTime;
    }
}
