package com.luruixiao.cep;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * 登录日志处理
 * @author luruixiao
 */
public class LoginLogAnalysis {
    /**
     * 从一堆的登录日志中，匹配一个恶意登录的模式（如果一个用户连续（在10秒内）失败三次，则是恶意登录），
     * 从而找到哪些用户名是恶意登录
     */
    public static void main(String[] args) throws Exception {
        //获取环境，选择流处理环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度
        streamEnv.setParallelism(1);
        //设置时间定义，使用事件时间
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取登录日志
        //指定EventTime的时候必须要确保是时间戳（精确到毫秒）
        ArrayList<LoginEvent> loginEvents = new ArrayList<>();
        loginEvents.add(new LoginEvent(1L, "张三", "fail", 1577080457000L));
        loginEvents.add(new LoginEvent(2L, "张三", "fail", 1577080458000L));
        loginEvents.add(new LoginEvent(3L, "张三", "fail", 1577080460000L));
        loginEvents.add(new LoginEvent(4L, "李四", "fail", 1577080458000L));
        loginEvents.add(new LoginEvent(5L, "李四", "success", 1577080462000L));
        loginEvents.add(new LoginEvent(6L, "张三", "fail", 1577080462000L));

        DataStreamSource<LoginEvent> dataStreamSource = streamEnv.fromCollection(loginEvents);
        //定义模式(Pattern)
//        Pattern<T, F> pattern = Pattern.<T>begin("start")
//                .next("middle").subtype(F.class)
//                .followedBy("end").where(new MyCondition());

//        Pattern pattern = (Pattern) Pattern.<LoginEvent>begin("start").where((IterativeCondition<LoginEvent>) loginEvent -> "fail".equals(loginEvent.ev));

        Pattern pattern = Pattern.<LoginEvent>begin("start").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) {
                return "fail".equals(loginEvent.eventType);
            }
        }).next("fail2").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) {
                return "fail".equals(loginEvent.eventType);
            }
        }).next("fail3").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) {
                return "fail".equals(loginEvent.eventType);
            }
        }).within(Time.seconds(10));


//                .next("fail2").where(_.eventType.equals("fail"))
//                .next("fail3").where(_.eventType.equals("fail"))
//                .within(Time.seconds(10)) //时间限制

        PatternStream patternStream = CEP.pattern(dataStreamSource.keyBy((KeySelector<LoginEvent, Object>) loginEvent -> loginEvent.userName), pattern);

        SingleOutputStreamOperator result = patternStream.select((PatternSelectFunction<LoginEvent, String>) map -> {
            Iterator<String> keyIter = map.keySet().iterator();
            LoginEvent e1 = map.get(keyIter.next()).iterator().next();
            LoginEvent e2 = map.get(keyIter.next()).iterator().next();
            LoginEvent e3 = map.get(keyIter.next()).iterator().next();
            return "用户名:" + e1.userName + "登录时间:" + e1.eventTime + ":" + e2.eventTime + ":" + e3.eventTime;
        });
        result.print();
        streamEnv.execute();
//        //检测Pattern
//        val patternStream: PatternStream[LoginEvent] = CEP.pattern(stream.keyBy(_.userName),pattern) //根据用户名分组
//
//        //选择结果并输出
//        val result: DataStream[String] = patternStream.select(new PatternSelectFunction[LoginEvent, String] {
//            override def select(map: util.Map[String, util.List[LoginEvent]]) = {
//                val keyIter: util.Iterator[String] = map.keySet().iterator()
//                val e1: LoginEvent = map.get(keyIter.next()).iterator().next()
//                val e2: LoginEvent = map.get(keyIter.next()).iterator().next()
//                val e3: LoginEvent = map.get(keyIter.next()).iterator().next()
//                "用户名:" + e1.userName + "登录时间:" + e1.eventTime + ":" + e2.eventTime + ":" + e3.eventTime
//            }
//        })
//        result.print()
//        streamEnv.execute()
    }
    static class LoginEvent{
        Long id;
        String userName;
        String eventType;
        Long eventTime;

        public LoginEvent() {
        }

        public LoginEvent(Long id, String userName, String eventType, Long eventTime) {
            this.id = id;
            this.userName = userName;
            this.eventType = eventType;
            this.eventTime = eventTime;
        }
    }
}
