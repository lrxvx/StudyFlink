package com.luruixiao.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import org.apache.flink.api.scala._
/**
  * 实时 flink data stream
  */
object DataStreamWcApp {

  def main(args: Array[String]): Unit = {

      val env = StreamExecutionEnvironment.getExecutionEnvironment

      // nc -lk 7777 nn1机器命令，然后输入数据
      val dataStream: DataStream[String] = env.socketTextStream("nn1", 7777)

      val result: DataStream[(String, Int)] = dataStream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_,1)).keyBy(0).sum(1)

      result.print()

      env.execute()

  }

}
