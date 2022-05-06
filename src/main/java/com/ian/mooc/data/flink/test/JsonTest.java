package com.ian.mooc.data.flink.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.List;

public class JsonTest {
    public static void main(String[] args) {
        testJson2();
    }
    public static void testJson2() {
        String record = new String(
                "{\"year\": 2015, \"month\": 8, \"day\": 1, \"hour\": 0,\"minute\": 6, " +
                        "\"info\":" + "[" +
                        "[\"pause_video\",214784,\"0ded37244e11cc47d584e252e49b45d6\",\"UQx/Think101x/_\",25]," +
                        "[\"play_video\",214784,\"0ded37244e11cc47d584e252e49b45d6\",\"UQx/Think101x/_\",26]]}");

        JSONObject object = JSONObject
                .parseObject(record);
        //string
        int year = object.getIntValue("year");
        String info = object.getString("info");
        System.out.println(year);
        System.out.println(info);

        //list

        //List<Integer> integers = JSON.parseArray(object.getJSONArray("list").toJSONString(),Integer.class);
        //integers.forEach(System.out::println);
        //null
        //System.out.println(object.getString("null"));


}}
