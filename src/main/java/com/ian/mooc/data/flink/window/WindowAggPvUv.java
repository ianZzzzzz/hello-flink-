package com.ian.mooc.data.flink.window;

import com.ian.mooc.data.flink.mapper.MapToEvent;
import com.ian.mooc.data.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.time.Duration;
// 没开始写
public class WindowAggPvUv {
    public static void main(String[] args) {
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
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(61))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.getTimestamp();
                                    }
                                }));

    }
}
