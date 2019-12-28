package com.luruixiao.flink.distribution

import com.luruixiao.flink.constant.Constants
import com.luruixiao.flink.utils.ConfigurationManager
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
/**
  */
object DistributionUserAggregation {

  def main(args: Array[String]): Unit = {

    val unity: Integer = ConfigurationManager.getInteger(Constants.DISTRIBUTION_UNITY)

    val startTime: String = ConfigurationManager.getProperty(Constants.DISTRIBUTION_START_TIME)

    val endTime: String = ConfigurationManager.getProperty(Constants.DISTRIBUTION_END_TIME)

    val event: String = ConfigurationManager.getProperty(Constants.DISTRIBUTION_EVENT)


    val argsParam: ParameterTool = ParameterTool.fromArgs(args)

    val inputPath: String = argsParam.get("input")

    val outputPath: String = argsParam.get("output")

    //1 env 2 source 3 transform 4 sink
    val env = ExecutionEnvironment.getExecutionEnvironment

    val txtDataSet: DataSet[String] = env.readTextFile(inputPath)


    val dataByClass: DataSet[(String, (String, String, Int))] = txtDataSet.filter(data => {
      data.split(";")(1) == event
    }).map(one => {
      val dataArr: Array[String] = one.split(";")
      val aid: String = dataArr(0)
      val dt: String = dataArr(2)
      val groupValue: String = dataArr(3)
      (groupValue, (aid, dt, 1))
    })
    dataByClass.groupBy(0)


//    val aggSet = txtDataSet.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)
//
//    aggSet.writeAsCsv(outputPath).setParallelism(1)

    env.execute()

  }


}
