package com.ian.mooc.data.flink.detector;

import com.ian.mooc.data.flink.pojo.Action;
import com.ian.mooc.data.flink.pojo.Event;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

//public class SceneDetector extends KeyedProcessFunction<Event,String,String> {

public class SceneDetector extends KeyedProcessFunction<Integer, Event, Alert> {

    private transient ValueState<Integer> flagState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Integer> flagDescriptor = new ValueStateDescriptor<>(
                "flag",
                Types.INT);
        flagState = getRuntimeContext().getState(flagDescriptor);

    }

    @Override
    public void processElement(
            Event event,
            Context context, //KeyedProcessFunction<Integer, Event, Alert>.Context context
            Collector<Alert> collector) throws Exception {
        Integer lastActionWasVideo = flagState.value();
        if (lastActionWasVideo != null){
            if(event.getActionType().equals(String.valueOf("question"))){
                Alert alert = new Alert();
                alert.setId(event.getUser());
                collector.collect(alert);
            }
            cleanUp(context);
        }
        if(!event.getActionType().equals(String.valueOf("question"))){
            flagState.update(Integer.parseInt("1"));

            long timer = context.timerService().currentProcessingTime() ;
            context.timerService().registerProcessingTimeTimer(timer);
        }
    }
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {

        flagState.clear();
    }

    private void cleanUp(Context ctx) throws Exception {
        // 发出告警后清除状态 alert-->empty
        flagState.clear();
    }
    }










