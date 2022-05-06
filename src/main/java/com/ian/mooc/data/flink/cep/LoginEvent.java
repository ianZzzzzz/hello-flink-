package com.ian.mooc.data.flink.cep;

public class LoginEvent {
    public long eventTime;
    public Object userId;
    public String eventType;
    public String ipAddress;

    public LoginEvent(String user_1, String s, String fail, long l) {

    }
}
