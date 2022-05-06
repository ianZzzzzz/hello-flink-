package com.ian.mooc.data.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

public class Consumer {
    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(25672);
        factory.setUsername("admin");
        factory.setPassword("sduhiawhdoqwh");
        factory.setVirtualHost("/");

        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            DeliverCallback deliverCallback = (consumerTag,message) -> {
                System.out.println(new String(message.getBody()));
            };
            CancelCallback cancelCallback = (consumerTag) ->{
                System.out.println("消费中断");
            };
            String java_test = channel.basicConsume("user_action", true, deliverCallback, cancelCallback);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

    }
}










