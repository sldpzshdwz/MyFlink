package com.example;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.FlatMapFunction;


public class wordcount {

    public static void main(String[] args) throws Exception {

        // 1. 创建执行环境
        ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();


        // 2. 从文件读取数据  按行读取(存储的元素就是每行的文本)
        DataSource<String> lineDs = env.readTextFile("input/words.txt");;

        // 3. 转换数据格式
        FlatMapOperator<String,Tuple2<String,Integer>> wordAndOne = lineDs.flatMap(new FlatMapFunction<String,Tuple2<String,Integer>>(){
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws java.lang.Exception
                    {
                        String[] words=line.split(" ");
                        for (String word:words){
                            Tuple2<String,Integer> wordTuple2 = Tuple2.of(word,1);
                            out.collect(wordTuple2);
                        }
                    }
                });;

        // 4. 按照 word 进行分组
        UnsortedGrouping<Tuple2<String,Integer>> groupBy = wordAndOne.groupBy(0);
        // 5. 分组内聚合统计
        AggregateOperator<Tuple2<String,Integer>> sum = groupBy.sum(1);
        // 6. 打印结果
        sum.print();
    }
}
