package com.luruixiao;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink 实时流WC处理
 * @author luruixiao
 */
public class FlinkStreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = environment.socketTextStream("node1", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = dataStream.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
            String[] split = s.split(",");
            for (String s1 : split) {
                collector.collect(new Tuple2<>(s1, 1));
            }
        }).keyBy(0).reduce((ReduceFunction<Tuple2<String, Integer>>) (t1, t2) -> new Tuple2<>(t1.f0, t1.f1 + t2.f1));
        result.print();
        environment.execute("wordcount");
    }
}
