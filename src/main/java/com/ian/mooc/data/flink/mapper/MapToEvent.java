package com.ian.mooc.data.flink.mapper;

import com.ian.mooc.data.flink.pojo.Event;
import org.apache.flink.api.common.functions.MapFunction;

public class MapToEvent implements MapFunction<String, Event> {
    @Override
    public Event map(String eventJsonStr) throws Exception {
        Event event = new Event(eventJsonStr);

        return event;
    }
}