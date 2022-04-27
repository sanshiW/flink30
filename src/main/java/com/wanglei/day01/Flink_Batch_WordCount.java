package com.wanglei.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class Flink_Batch_WordCount {
    public static void main(String[] args) throws Exception {
        //创建批处理环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //从文件读取数据
        DataSource<String> datasource = env.readTextFile("input/word.txt");

        /**
         * 首先flatMap（按照空格切分，然后组成Tuple2元组（word，1））
         * 其次ReduceByKey（1.先将相同的单词聚和到一块 2.再做累加）
         * 最后把结果输出到控制台
         */
        FlatMapOperator<String, Tuple2<String, Integer>> wordToOne = datasource.flatMap(new MyFlatMap());

        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOne.groupBy(0);

        AggregateOperator<Tuple2<String, Integer>> res = groupBy.sum(1);

        res.print();


    }

    private static class MyFlatMap implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
//                out.collect(new Tuple2<>(word,1));
                out.collect(Tuple2.of(word,1));
            }
        }
    }
}
