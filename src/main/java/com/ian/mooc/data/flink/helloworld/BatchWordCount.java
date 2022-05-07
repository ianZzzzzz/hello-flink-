package com.ian.mooc.data.flink.helloworld;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class BatchWordCount {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env2 = ExecutionEnvironment.getExecutionEnvironment();
        String localPath="src/main/java/input/word.txt";
        String cloudPath="/data/word.txt";
        DataSource<String> lineDataSource = env2.readTextFile(cloudPath);
        FlatMapOperator<String, Tuple2<String, Long>> wordandOne = lineDataSource.flatMap(
                (String line, Collector<Tuple2<String, Long>> out) ->

                {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }
        ).returns(Types.TUPLE(Types.STRING, Types.LONG));

        AggregateOperator<Tuple2<String, Long>> groupByResult = wordandOne.groupBy(0).sum(1);

        //groupByResult.print(); 部署要关闭


    }
}
