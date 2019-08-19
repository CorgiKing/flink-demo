package org.goaler.flinkdemo;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

public class WikipediaAnalysis {

    public static void main(String[] args) throws Exception{
        //创建上下文环境
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        //添加数据源
        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

        KeyedStream<WikipediaEditEvent, String> keyedStream = edits.keyBy(new KeySelector<WikipediaEditEvent, String>() {
            public String getKey(WikipediaEditEvent event) throws Exception {
                return event.getUser();
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> fold = keyedStream.timeWindow(Time.seconds(1))
                .fold(new Tuple2<String, Integer>("", 0), new FoldFunction<WikipediaEditEvent, Tuple2<String, Integer>>() {
                    public Tuple2<String, Integer> fold(Tuple2<String, Integer> acc, WikipediaEditEvent event) throws Exception {
                        acc.f0 = event.getUser();
                        acc.f1 += event.getByteDiff();
                        return acc;
                    }
                });

        fold.print();

        see.execute();
    }
}
