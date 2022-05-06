package com.ian.mooc.data.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class Producer {
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("172.17.0.6");
        factory.setPort(5672);
        factory.setUsername("ian");
        factory.setPassword(String.valueOf(1));
        factory.setVirtualHost("/");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare("java_test2",false,true,true,null);
        channel.basicPublish("","java_test2",null,"hello_message2".getBytes());

    }
}
