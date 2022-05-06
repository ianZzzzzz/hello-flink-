package com.ian.mooc.java.stream.pojo;

public class Student {
    int no;
    String name;
    String sex;
    float height;

    public Student(int no, String name, String sex, float height) {
        this.no = no;
        this.name = name;
        this.sex = sex;
        this.height = height;
    }

    public String getSex() {
        return this.sex;
    }
}
