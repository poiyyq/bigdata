
package com.winnie.java;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCount {

    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + ": " + count;
        }
    }

    public static void main(String[] args) throws Exception {
// 先开启本地服务  nc -lk 9999  ，再去测试
        // 1. 解析外部参数,获取要监听的主机、端口,没有配置则取默认值localhost:9999
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "localhost");
        int port = tool.getInt("port", 9999);

        // 2. 获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 3. 初始化数据
        DataStreamSource<String> source = env.socketTextStream(host, port);

        // 4. 计算,扁平化,每个单次计数为1,分组,累加次数
        SingleOutputStreamOperator<WordWithCount> counts = source.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String line, Collector<WordWithCount> collector) throws Exception {
                String[] words = line.split("\\s+");
                for(String word : words) {
                    collector.collect(new WordWithCount(word, 1L));
                }

            }
        }).keyBy("word")
                .timeWindow(Time.seconds(2), Time.seconds(1))
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount word1, WordWithCount word2) throws Exception {
                        return new WordWithCount(word1.word, word1.count + word2.count);
                    }
                });
        /*
         * 如果只是简单的相加，可以直接使用sum()方法
         * .keyBy("word").timeWindow(Time.seconds(2), Time.seconds(1)).sum("count")
         */

        // 5. 打印结果,设置并行度
        counts.print().setParallelism(1);

        // 6. 开启流任务,这是一个action算子,将触发计算
        env.execute("SocketWindowWordCountJava");

    }

}