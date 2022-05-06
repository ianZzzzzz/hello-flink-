package com.ian.mooc.data.flink.source;

import com.ian.mooc.data.flink.pojo.Action;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Action> actionDataStreamSource = env.fromElements(
                new Action("click_info", 101, "courseId-01", "sessionId-01"),
                new Action("play_video", 101, "courseId-01", "sessionId-01"),
                new Action("click_about", 101, "courseId-01", "sessionId-01")

        );

        actionDataStreamSource.print("1");
        env.execute();

    }
}
