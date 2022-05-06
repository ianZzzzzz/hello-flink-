package com.ian.mooc.data.flink.source;

import com.ian.mooc.data.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class MyAssigner
        implements
            org.apache.flink.api.common.eventtime.WatermarkStrategy<com.ian.mooc.data.flink.pojo.Event>,
            org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks<com.ian.mooc.data.flink.pojo.Event>,
            org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks<com.ian.mooc.data.flink.pojo.Event>
{

    @Override
    public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return null;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return null;
    }

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(Event event, long l) {
        return null;
    }

    @Override
    public long extractTimestamp(Event event, long l) {
        return 0;
    }
}
