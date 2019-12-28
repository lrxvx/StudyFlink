package com.luruixiao.flink

import com.luruixiao.flink.constant.Constants
import com.luruixiao.flink.utils.{ConfigurationManager, MyKafkaUtil}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.scala._
/**
  * flink 流api 使用，kafka
  */
object StreamApiApp {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val topic: String = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS)
    val flnkKafkaConsumer: FlinkKafkaConsumer011[String] = MyKafkaUtil.getKafkaSource(topic)

    val dstream: DataStream[String] = env.addSource(flnkKafkaConsumer)

    dstream.print()

    env.execute()
  }
}
