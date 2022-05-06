package com.ian.mooc.data.flink.cep;

import com.ian.mooc.data.flink.mapper.MapToEvent;
import com.ian.mooc.data.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.util.List;
import java.util.Map;

public class CepExample {
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


        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<LoginEvent> stream = env
                .fromElements(
                        new LoginEvent("user_1", "0.0.0.0", "fail", 2000L),
                        new LoginEvent("user_1", "0.0.0.1", "fail", 3000L),
                        new LoginEvent("user_1", "0.0.0.2", "fail", 4000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<LoginEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                                    @Override
                                    public long extractTimestamp(LoginEvent loginEvent, long l) {
                                        return loginEvent.eventTime;
                                    }
                                })
                )
                .keyBy(r -> r.userId);

        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("first")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                .next("second")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                .next("third")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                .within(Time.seconds(5));


        PatternStream<LoginEvent> patternedStream = CEP.pattern(stream, pattern);

        patternedStream
                .select(new PatternSelectFunction<LoginEvent, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> select(Map<String, List<LoginEvent>> map) throws Exception {
                        LoginEvent first = map.get("first").iterator().next();
                        LoginEvent second = map.get("second").iterator().next();
                        LoginEvent third = map.get("third").iterator().next();
                       // return Tuple4.of(first.userId, first.ipAddress, second.ipAddress, third.ipAddress);
                        return Tuple4.of("","","","");
                    }
                })
                .print();

        env.execute();
    }
}
