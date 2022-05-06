package com.ian.mooc.data.flink.cep;

import com.ian.mooc.data.flink.pojo.Event;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.walkthrough.common.entity.Alert;

import java.util.List;
import java.util.Map;

public class MyPatton {


    Pattern<Event, ?> pattern = Pattern.<Event>begin("start")
            .next("middle").where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event value) throws Exception {
                    return value.getActionType().equals("error");
                }
            }).followedBy("end").where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event value) throws Exception {
                    return value.getActionType().equals("critical");
                }
            }).within(Time.minutes(1));



}