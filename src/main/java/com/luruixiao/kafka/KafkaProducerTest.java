package com.luruixiao.kafka;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.luruixiao.flink.utils.ConfigurationManager;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


/**
 * dev分支修改
 * 一些修改等等，
 * @author luruixiao
 */
public class KafkaProducerTest implements Runnable {

    private final KafkaProducer<String, String> producer;
    private final String topic;
    public KafkaProducerTest(String topicName) {
        String servers = ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST);
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
//        auto.create.topics.enable=true
        ///增加的了新的功能
        this.producer = new KafkaProducer<String, String>(props);
        this.topic = topicName;
    }

    @Override
    public void run() {
        try {
            File log = new File("E:\\下载\\SogouQ.mini\\SogouQ.sample");
            List<String> logDatas = FileUtils.readLines(log, "gbk");
            for (String logData : logDatas) {
                Thread.sleep(1000L);
                producer.send(new ProducerRecord<String, String>(topic, "Message", logData));
            }
//            for(;;) {
//                String messageStr="你好，这是第"+messageNo+"条数据";
//                producer.send(new ProducerRecord<String, String>(topic, "Message", messageStr));
//                //生产了100条就打印
//                if(messageNo % 100==0){
//                    System.out.println("发送的信息:" + messageStr);
//                }
//                //生产1000条就退出
//                if(messageNo % 1000==0){
//                    System.out.println("成功发送了"+messageNo+"条");
//                    break;
//                }
//                messageNo++;
//            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
    public static void main(String[] args) {
        String topic = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        KafkaProducerTest test = new KafkaProducerTest(topic);
        ThreadPoolExecutor pool = new ThreadPoolExecutor(4, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(10));
        pool.execute(test);
        pool.shutdown();
    }
}
