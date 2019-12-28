package com.luruixiao.flink.utils

import java.lang
import java.util.Properties

import com.luruixiao.flink.constant.Constants
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * 获取kafka 信息工具
  */
object MyKafkaUtil {

  val prop = new Properties()

  val kafkaList: lang.String = ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST)
  prop.setProperty("bootstrap.servers",kafkaList)
  prop.setProperty("group.id","gmall")




  def getKafkaSource(topic: String): FlinkKafkaConsumer011[String] = {
    val kafkaSource: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(),prop)
    kafkaSource
  }

}
