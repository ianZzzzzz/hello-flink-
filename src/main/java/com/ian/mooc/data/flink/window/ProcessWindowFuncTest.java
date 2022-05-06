package com.ian.mooc.data.flink.window;


import com.ian.mooc.data.flink.pojo.Event;
import com.ian.mooc.data.flink.source.RabbitSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class ProcessWindowFuncTest {
    public static void main(String[] args) throws Exception {

        RabbitSource rabbitSource = new RabbitSource();
        rabbitSource.initRmqConnect(true);
        DataStream<Event> stream = rabbitSource.getEventStream();
        SingleOutputStreamOperator<Event> eventStreamWithWatermark = stream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.getTimestamp();
                                    }
                                }
                        )
        );
        eventStreamWithWatermark.print("[ eventStreamWithWatermark ]");
        SingleOutputStreamOperator<String> myProcessStream = eventStreamWithWatermark
                .keyBy(data -> true)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new MyProcessFunc());
        myProcessStream.print();


        rabbitSource.getEnv().execute();
    }
    public static class MyProcessFunc extends ProcessWindowFunction<Event,String,Boolean, TimeWindow>{
        @Override
        public void process(
                Boolean aBoolean,
                ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context,
                Iterable<Event> iterable,
                Collector<String> collector) throws Exception {
            HashSet<Integer> userSet = new HashSet<>();
            for (Event event:iterable){
                userSet.add(event.getUser());
            }
            int uniqueView = userSet.size();

            long start = context.window().getStart();
            long end = context.window().getEnd();
            collector.collect(
                    "[ window ]"+new Timestamp(start)+"~"+new Timestamp(end)
                            +", Unique View : "+uniqueView);
        }

    }
    public static class SceneExtractProcess extends ProcessWindowFunction<Event,String,Integer,TimeWindow>{

        @Override
        public void process(
                Integer integer,
                ProcessWindowFunction<Event, String, Integer, TimeWindow>.Context context,
                Iterable<Event> iterable,
                Collector<String> collector
        ) throws Exception {


            long start = context.window().getStart();
            long end = context.window().getEnd();
            collector.collect(
                    "[ window ]"+new Timestamp(start)+"~"+new Timestamp(end)
                            +", Scene : ");
        }

    }
}
