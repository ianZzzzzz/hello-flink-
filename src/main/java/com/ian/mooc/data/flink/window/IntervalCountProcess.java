package com.ian.mooc.data.flink.window;

import com.ian.mooc.data.flink.pojo.Event;
import com.ian.mooc.data.flink.pojo.Interval;
import com.ian.mooc.data.flink.source.RabbitSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;

public class IntervalCountProcess {
    public static void main(String[] args) throws Exception {
        RabbitSource rabbitSource = new RabbitSource();
        DataStream<Event> eventStream = rabbitSource.getEventStreamTest(true);
        eventStream.print(" eventStream ");
        eventStream
                .keyBy(Event::getUser)
                        .process(new IntervalProcess()).print(" IntervalProcess ");

        rabbitSource.getEnv().execute();
    }
    public static class IntervalProcess extends KeyedProcessFunction<Integer,Event, Interval>{
        private LocalDateTime previousEventDateTime,nowEventDateTime;
        private Long timestampInterval;
        private Long previousTimestamp;

        @Override
        public void open(Configuration parameters) throws Exception {
            previousTimestamp = 0L;
            timestampInterval = 0L;
        }

        @Override
        public void processElement(
                Event event,
                KeyedProcessFunction<Integer, Event, Interval>.Context context,
                Collector<Interval> collector
        ) throws Exception {
            Long nowTimestamp = event.getTimestamp();
            if (previousTimestamp == 0L){
                previousTimestamp = nowTimestamp;
            }else{
                timestampInterval = nowTimestamp -previousTimestamp;
                // 区分长短间隔的阈值
                int thresholdBetweenLongAndShortInterval = 60 * 1000;
                String intervalType;
                if (timestampInterval > thresholdBetweenLongAndShortInterval)
                    intervalType = "LongInterval";
                else intervalType = "ShortInterval";
                Interval interval = new Interval(
                        timestampInterval,
                        intervalType, context.getCurrentKey(),
                        event.getCourse(),
                        event.getSession());
                previousTimestamp = nowTimestamp;
                collector.collect(interval);
            }
        }
    }

}
















