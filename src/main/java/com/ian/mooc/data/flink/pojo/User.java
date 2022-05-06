package com.ian.mooc.data.flink.pojo;

public class User {
    public int id;
    public boolean gender;
    public int age;
    public int educate;

    public User() {
    }

    public User(int id, boolean gender, int age, int educate) {
        this.id = id;
        this.gender = gender;
        this.age = age;
        this.educate = educate;
    }
}
