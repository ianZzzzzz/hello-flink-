package com.ian.mooc.data.flink.hw;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String, Long>> operator = stringDataStreamSource.flatMap(

                (String line, Collector<Tuple2<String, Long>> out) ->
                {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }

        ).returns(Types.TUPLE(Types.STRING,Types.LONG));

        SingleOutputStreamOperator<Tuple2<String, Long>> result = operator.keyBy(data -> data.f0).sum(1);
        //result.print();
        env.execute();

    }
}
