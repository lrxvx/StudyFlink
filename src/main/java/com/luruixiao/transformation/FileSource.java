package com.luruixiao.transformation;

import com.luruixiao.stream.WindowWordCount;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 读文件，可以是本地机器，也可以是HDFS上，这里为方面直接读本地文件
 * @author luruixiao
 */
public class FileSource {

    public static void main(String[] args) throws Exception {
        //初始化Flink的Streaming上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取数据
        DataStreamSource<String> dataStreamSource = env.readTextFile("F:\\workpace\\scala\\StudyFlink\\src\\main\\resources\\wc.txt");
        
        //转化数据
        dataStreamSource.flatMap(new WindowWordCount.Splitter()).keyBy(0).sum(1).print();

        //启动流处理
        env.execute("fileSource");
    }
}
