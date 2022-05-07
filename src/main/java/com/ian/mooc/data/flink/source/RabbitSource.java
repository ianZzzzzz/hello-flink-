package com.ian.mooc.data.flink.source;
import com.ian.mooc.data.flink.mapper.MapToEvent;
import com.ian.mooc.data.flink.pojo.Action;
import com.ian.mooc.data.flink.pojo.Event;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public class RabbitSource {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    boolean local=true;

    public StreamExecutionEnvironment getEnv(){return env;}
    public DataStream<Event> getEventStream(boolean local){
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
        return env.addSource(rmqSource).map(new MapToEvent());
    }
    public DataStream<Event> getEventStreamTest(boolean local){

        return env.fromElements(
                new Event("{'year': 2015, 'month': 1, 'day': 1, 'hour': 0, 'minute': 0, 'second': 0, 'actionType': 'A', 'user': '1', 'session': 'session1', 'course': 'course1'}"),
                new Event("{'year': 2015, 'month': 1, 'day': 1, 'hour': 0, 'minute': 0, 'second': 0, 'actionType': 'A', 'user': '2', 'session': 'session2', 'course': 'course2'}"),
                new Event("{'year': 2015, 'month': 1, 'day': 1, 'hour': 0, 'minute': 0, 'second': 10, 'actionType': 'B', 'user': '1', 'session': 'session1', 'course': 'course1'}"),
                new Event("{'year': 2015, 'month': 1, 'day': 1, 'hour': 0, 'minute': 0, 'second': 10, 'actionType': 'A', 'user': '2', 'session': 'session2', 'course': 'course2'}"),
                new Event("{'year': 2015, 'month': 1, 'day': 1, 'hour': 0, 'minute': 0, 'second': 20, 'actionType': 'C', 'user': '1', 'session': 'session1', 'course': 'course1'}"),
                new Event("{'year': 2015, 'month': 1, 'day': 1, 'hour': 0, 'minute': 0, 'second': 20, 'actionType': 'A', 'user': '2', 'session': 'session2', 'course': 'course2'}"),
                new Event("{'year': 2015, 'month': 1, 'day': 1, 'hour': 0, 'minute': 0, 'second': 30, 'actionType': 'A', 'user': '1', 'session': 'session1', 'course': 'course1'}"),
                new Event("{'year': 2015, 'month': 1, 'day': 1, 'hour': 0, 'minute': 0, 'second': 30, 'actionType': 'A', 'user': '2', 'session': 'session2', 'course': 'course2'}"),
                new Event("{'year': 2015, 'month': 1, 'day': 1, 'hour': 0, 'minute': 0, 'second': 40, 'actionType': 'B', 'user': '1', 'session': 'session1', 'course': 'course1'}"),
                new Event("{'year': 2015, 'month': 1, 'day': 1, 'hour': 0, 'minute': 0, 'second': 40, 'actionType': 'A', 'user': '2', 'session': 'session2', 'course': 'course2'}"),
                new Event("{'year': 2015, 'month': 1, 'day': 1, 'hour': 0, 'minute': 0, 'second': 50, 'actionType': 'C', 'user': '1', 'session': 'session1', 'course': 'course1'}"),
                new Event("{'year': 2015, 'month': 1, 'day': 1, 'hour': 0, 'minute': 1, 'second': 50, 'actionType': 'A', 'user': '2', 'session': 'session2', 'course': 'course2'}")

        );


    }

}
