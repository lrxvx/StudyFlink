package com.luruixiao.flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * flink data set 离线 word count
  */
object DataSetWcApp {

  def main(args: Array[String]): Unit = {

    val argsParam: ParameterTool = ParameterTool.fromArgs(args)

    val inputPath: String = argsParam.get("input")

    val outputPath: String = argsParam.get("output")

    //1 env 2 source 3 transform 4 sink
    val env = ExecutionEnvironment.getExecutionEnvironment

    val txtDataSet = env.readTextFile(inputPath)

    val aggSet = txtDataSet.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)

    aggSet.writeAsCsv(outputPath).setParallelism(1)

    env.execute()

  }
}
