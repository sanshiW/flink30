package com.wanglei.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink_Stream_WordCount2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度为1 方便观察
        env.setParallelism(1);

        //读取无界数据
        DataStreamSource<String> dataSource = env.socketTextStream("hadoop102", 9999);

        //按照空格切分 并取出每一个单词
        SingleOutputStreamOperator<String> wordStream = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                //遍历数组 取出每个单词
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //将每一个单词组成Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneStream = wordStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        //将相同的单词聚合到一起
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneStream.keyBy(0);

        //做累加操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyedStream.sum(1);

        //打印到控制台
        res.print("累加结果");

        //执行代码
        env.execute();


    }

}
