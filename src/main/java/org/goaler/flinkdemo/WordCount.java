package org.goaler.flinkdemo;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class WordCount {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.fromElements(
                "Using the Hello World guide",
                " you’ll start a branch",
                " write comments",
                "Using the Hello World guide",
                " you’ll start a branch",
                " write comments",
                "and open a pull request. ");
        //从文件获取数据
//        DataSet<String> text = env.readTextFile("d:/test.log");

        DataSet<Tuple2<String, Integer>> sum = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collectors) {
                String[] tokens = value.toLowerCase().split("\\W+");
                for (String token : tokens) {
                    if (token.length() > 0) {
                        collectors.collect(new Tuple2<String, Integer>(token, 1));
                    }
                }
            }
        }).groupBy(0).sum(1);

        long st = System.currentTimeMillis();
        sum.print();
        System.out.println(System.currentTimeMillis() - st);

    }

}
