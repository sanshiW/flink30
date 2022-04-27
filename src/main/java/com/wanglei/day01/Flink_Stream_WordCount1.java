package com.wanglei.day01;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink_Stream_WordCount1 {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度为1
        env.setParallelism(1);

        //从文件读取数据
        DataStreamSource<String> dataSource = env.readTextFile("input/word.txt");

        /**
         *  首先flatMap（按照空格切分，然后组成Tuple2元组（word，1））
         *          * 其次ReduceByKey（1.先将相同的单词聚和到一块 2.再做累加）
         *          * 最后把结果输出到控制台
         */
        //按照空格切分 然后组成Tuple2元组（word，1）
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneStream = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //按照空格切分
                String[] words = value.split(" ");

                //遍历出每个单词
                for (String word : words) {
                    //返回Tuple2元组
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        //将相同的单词聚合一起
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneStream.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        //做累加计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyedStream.sum(1);

        //打印到控制台
        res.print();

        //执行代码
        env.execute();

    }
}
