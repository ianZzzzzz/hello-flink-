package com.ian.mooc.data.flink.source;
import com.ian.mooc.data.flink.mapper.MapToEvent;
import com.ian.mooc.data.flink.pojo.Event;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public class RabbitSource {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    boolean local=true;
    String rabbitIP ;
    int rabbitPort ;
    public void initRmqConnect( boolean local) {
        if (local){
            rabbitIP = "127.0.0.1";
            rabbitPort = 25672;}
        else {
            rabbitIP = "172.29.0.4";
            rabbitPort = 5672;}


    }
    public StreamExecutionEnvironment getEnv(){
        return env;
    }
    public DataStream<Event> getEventStream(){

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


}
