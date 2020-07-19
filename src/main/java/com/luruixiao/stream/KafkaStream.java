package com.luruixiao.stream;


import com.luruixiao.flink.utils.ConfigurationManager;
import com.luruixiao.kafka.Constants;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        String groupId = ConfigurationManager.getProperty(Constants.KAFKA_GROUP_ID);
        String topic = "sougou_user_log";
//        String topic = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
//        String servers = ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node2:9092,node3:9092,node4:9092");
        properties.setProperty("group.id", "flinkStreamGroup");
//        properties.setProperty("enable.auto.commit", "true");
//        properties.setProperty("auto.commit.interval.ms", "1000");
//        properties.setProperty("session.timeout.ms", "30000");
//        properties.setProperty("auto.offset.reset", "earliest");
//        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
//        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        DataStreamSource<String> dataStreamSource = env.
                addSource(new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), properties));

        dataStreamSource.print();

        env.execute("kafka connect");
    }
}
