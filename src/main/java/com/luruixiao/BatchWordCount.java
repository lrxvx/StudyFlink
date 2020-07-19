package com.luruixiao;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理
 * @author luruixiao
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = environment.readTextFile("F:\\workpace\\scala\\StudyFlink\\src\\main\\resources\\wc.txt");
        dataSource.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
            String[] split = s.split(" ");
            for (String s1 : split) {
                collector.collect(new Tuple2<>(s1, 1));
            }
        }).groupBy(0).sum(1).print();
    }
}
