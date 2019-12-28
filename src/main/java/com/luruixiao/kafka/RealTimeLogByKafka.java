package com.luruixiao.kafka;

import com.luruixiao.flink.utils.ConfigurationManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author luruixiao
 */
public class RealTimeLogByKafka {

    public static void main(String[] args) {
        String grouoId = ConfigurationManager.getProperty(Constants.KAFKA_GROUP_ID);
        String topic = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String servers = ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST);
        KafkaConsumerTest test = new KafkaConsumerTest(servers, grouoId, topic);
        ThreadPoolExecutor pool = new ThreadPoolExecutor(4, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(10));
        pool.execute(test);
        pool.shutdown();
    }

}
class KafkaConsumerTest implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private ConsumerRecords<String, String> msgList;
    private final String topic;


    public KafkaConsumerTest(String servers, String groupId, String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<String, String>(props);
        this.topic = topicName;
        this.consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        System.out.println("---------开始消费---------");
        try {
            for (;;) {
                msgList = consumer.poll(1000);
                if(null != msgList && msgList.count() > 0){
                    for (ConsumerRecord<String, String> record : msgList) {
                        System.out.println("=======receive: key = " + record.key() + ", value = " + record.value()+" offset==="+record.offset());
                    }
                } else {
                    Thread.sleep(1000);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
