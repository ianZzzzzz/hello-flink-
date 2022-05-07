package com.ian.mooc.data.flink.window;


import com.ian.mooc.data.flink.pojo.Event;
import com.ian.mooc.data.flink.pojo.Scene;
import com.ian.mooc.data.flink.source.RabbitSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashMap;

public class SceneCountProcess {
    public static void main(String[] args) throws Exception {

        RabbitSource rabbitSource = new RabbitSource();

        DataStream<Event> stream = rabbitSource.getEventStreamTest(true);
        SingleOutputStreamOperator<Event> eventStreamWithWatermark = stream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(20)) // 设置水位线策略
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<Event>) (event, l) -> event.getTimestamp()
                                )
                );


        SingleOutputStreamOperator<Scene> sceneStream = eventStreamWithWatermark
                .keyBy(Event::getUser)
                .process(new ProcessActionToScene());



        SingleOutputStreamOperator<Tuple2<Tuple2<Integer, String>, Integer>> sceneSum = sceneStream
                .map(new MapForSceneCount())
                .keyBy(tuple -> tuple.f0.f0)
                .keyBy(tuple -> tuple.f0.f1)
                .sum(1);
        sceneSum.print(" sceneSum ");

        rabbitSource.getEnv().setParallelism(1).execute();
    }

    public static class MyProcess extends ProcessWindowFunction<Event, String, Integer, TimeWindow> {
        @Override
        public void process(
                Integer user,
                ProcessWindowFunction<Event, String, Integer, TimeWindow>.Context context,
                Iterable<Event> iterable,
                Collector<String> collector) throws Exception {


            HashMap<String, Integer> sceneCount = new HashMap<>();
            String actionPre = "";
            int oldCount, newCount;
            for (Event event : iterable) {
                System.out.println("[ user id ] : " + event.getUser());
                String actionNow = event.getActionType();
                String scene = actionPre + actionNow;
                actionPre = actionNow;
                try {
                    oldCount = sceneCount.get(scene);
                    newCount = oldCount + 1;
                    sceneCount.replace(scene, newCount);
                } catch (Exception e) {
                    sceneCount.put(scene, 1);

                }
            }


            long start = context.window().getStart();
            long end = context.window().getEnd();
            collector.collect(
                    "[ window ]" + new Timestamp(start) + "~" + new Timestamp(end)
                            + ", Scene count : " + sceneCount);
        }

    }

    public static class MyProcess2 extends ProcessWindowFunction<Event, Integer, Integer, TimeWindow> {

        @Override
        public void process(
                Integer integer,
                ProcessWindowFunction<Event, Integer, Integer, TimeWindow>.Context context,
                Iterable<Event> iterable,
                Collector<Integer> collector) throws Exception {
            System.out.println("[ in ]");
            for (Event event : iterable) {
                System.out.println("[ process ] : " + event.getUser());
            }
        }
    }

    public static class MapForSceneCount implements MapFunction<Scene, Tuple2<Tuple2<Integer, String>, Integer>> {
        @Override
        public Tuple2<Tuple2<Integer, String>, Integer> map(Scene scene) throws Exception {
            return Tuple2.of(Tuple2.of(scene.getUser(), scene.getSceneType()), 1);
        }
    }

    public static class ProcessActionToScene extends KeyedProcessFunction<Integer, Event, Scene> {
        String preAction, nowAction, sceneType;


        @Override
        public void open(Configuration parameters) throws Exception {
            preAction = "";
        }

        @Override
        public void processElement(
                Event event,
                KeyedProcessFunction<Integer, Event, Scene>.Context context,
                Collector<Scene> collector
        ) throws Exception {
            if (preAction.equals("")) {
                preAction = event.getActionType();
            } else {
                nowAction = event.getActionType();
                sceneType = preAction + "->" + nowAction;
                Scene scene = new Scene(sceneType, context.getCurrentKey(), event.getCourse(), event.getSession());
                preAction = nowAction;
                collector.collect(scene);
            }
        }
    }
}























