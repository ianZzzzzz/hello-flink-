package com.ian.mooc.data.flink.watermark;

import com.ian.mooc.data.flink.mapper.MapToEvent;
import com.ian.mooc.data.flink.pojo.Event;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collection;

public class WaterMarkTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        boolean local=true;
        String rabbitIP ;
        int rabbitPort ;
        if (local){
            rabbitIP = "127.0.0.1";
            rabbitPort = 25672;}
        else {
            rabbitIP = "172.29.0.4";
            rabbitPort = 5672;}
        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(rabbitIP).setPort(rabbitPort)
                .setVirtualHost("/").setUserName("mooc").setPassword("sduhiawhdoqwh")
                .build();
        RMQSource<String> rmqSource = new RMQSource<>(
                connectionConfig,
                "user_action",
                false,
                new SimpleStringSchema());
        DataStream<Event> eventDataStream = env.addSource(rmqSource).map(new MapToEvent());

        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = eventDataStream
                .assignTimestampsAndWatermarks(
//                有序/单调流的时间 实践中由于分布式的原因都为乱序数据
//                WatermarkStrategy.<Event>forMonotonousTimestamps()
//                .withTimestampAssigner(
//                        new SerializableTimestampAssigner<Event>() {
//                            @Override
//                            public long extractTimestamp(Event event, long l) {
//                                return event.getTimestamp();
//                            }})
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(61))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.getTimestamp();
                                    }
                                }));

        eventSingleOutputStreamOperator.print("[ eventSingleOutputStreamOperator ] : ");
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> afterMap = eventSingleOutputStreamOperator.map(new MapFunction<Event, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Event event) throws Exception {
                return Tuple2.of(event.getUser(), 1);
            }});
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> countPV = eventSingleOutputStreamOperator
                .map(new MapFunction<Event, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(Event event) throws Exception {
                        return Tuple2.of(event.getUser(), 1);
                    }
                })
                .keyBy(data -> data.f0)
                .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(30)))
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) throws Exception {
                        return Tuple2.of(t1.f0, t1.f1 + t2.f1);
                    }
                });
        // 其他window
                //TumblingEventTimeWindows.of(Time.minutes(1)));//offset 可以用来矫正时差
                //  SlidingEventTimeWindows.of()
                //EventTimeSessionWindows.withGap(Time.hours(1)))
                //        int countNum=10;
                //        int slideLength = 2;
                //        WindowedStream<Event, Integer, GlobalWindow> countWindowStream = eventDataStream.keyBy(Event::getUser).countWindow(countNum, slideLength);1   w




        env.execute("assign timestamp and test getTimestamp in even pojo.");


    }
}
















