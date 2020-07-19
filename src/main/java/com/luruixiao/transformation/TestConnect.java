package com.luruixiao.transformation;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;


public class TestConnect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> streamSource1 = executionEnvironment.fromElements(new Tuple2<>("a", 1), new Tuple2<>("b", 2), new Tuple2<>("c", 3));
        DataStreamSource<String> streamSource2 = executionEnvironment.fromElements("e", "f", "g");
        ConnectedStreams<Tuple2<String, Integer>, String> connect = streamSource1.connect(streamSource2);
        SingleOutputStreamOperator<Object> result = connect.map(new CoMapFunction<Tuple2<String, Integer>, String, Object>() {
            @Override
            public Object map1(Tuple2<String, Integer> stringIntegerTuple2) {
                return new Tuple2<>(stringIntegerTuple2.f0,stringIntegerTuple2.f1);
            }

            @Override
            public Object map2(String s) {
                return new Tuple2<>(s, 0);
            }
        });
        result.print();
        executionEnvironment.execute();
    }
}
