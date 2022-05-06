package com.ian.mooc.java.stream;

import com.ian.mooc.java.stream.pojo.Student;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StreamHW {

    public static void main(String[] args) {

        Student stuA = new Student(1, "A", "M", 184);
        Student stuB = new Student(2, "B", "G", 163);
        Student stuC = new Student(3, "C", "M", 175);
        Student stuD = new Student(4, "D", "G", 158);
        Student stuE = new Student(5, "E", "M", 170);
        ArrayList<Student> list = new ArrayList<>();
        list.add(stuA);
        list.add(stuB);
        list.add(stuC);
        list.add(stuD);
        list.add(stuE);
        Iterator<Student> iterator = list.iterator();

        while(iterator.hasNext()) {
            Student stu = iterator.next();
            if (stu.getSex().equals("G")) {

                System.out.println(stu.toString());


            }}
        list.stream()
                .filter(student -> student.getSex().equals("G"))
                .forEach(student -> System.out.println(student.toString()));



    }


}