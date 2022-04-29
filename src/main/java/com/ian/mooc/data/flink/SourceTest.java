package com.ian.mooc.data.flink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Action> actionDataStreamSource = env.fromElements(
                new Action("click_info", "2015/08/31 20:59:59", 101, "courseId-01", "sessionId-01"),
                new Action("play_video", "2015/08/31 21:00:1", 101, "courseId-01", "sessionId-01"),
                new Action("click_about", "2015/08/31 21:00:29", 101, "courseId-01", "sessionId-01"),
                new Action("stop_info", "2015/08/31 21:00:59", 101, "courseId-01", "sessionId-01"),

                new Action("click_info", "2015/08/31 20:59:59", 102, "courseId-01", "sessionId-01"),
                new Action("play_video", "2015/08/31 21:00:1", 102, "courseId-01", "sessionId-01"),
                new Action("click_about", "2015/08/31 21:00:29", 102, "courseId-01", "sessionId-01"),
                new Action("stop_info", "2015/08/31 21:00:59", 102, "courseId-01", "sessionId-01"),

                new Action("click_info", "2015/08/31 20:59:59", 103, "courseId-02", "sessionId-03"),
                new Action("play_video", "2015/08/31 21:00:1", 103, "courseId-02", "sessionId-03"),
                new Action("click_about", "2015/08/31 21:00:29", 103, "courseId-02", "sessionId-03"),
                new Action("stop_info", "2015/08/31 21:00:59", 103, "courseId-02", "sessionId-03")
        );

        actionDataStreamSource.print("1");
        env.execute();

    }
}
