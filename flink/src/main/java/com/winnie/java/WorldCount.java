package com.winnie.java;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.util.Collector;

import java.util.List;

public class WorldCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        String textFile = "/Users/admin/code/bigdata/flink/src/main/resources/worldcount.txt";
        DataSource<String> stringDataSource = executionEnvironment.readTextFile(textFile);
        FlatMapOperator<String, WordWithCount> flatMapOperator = stringDataSource.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String line, Collector<WordWithCount> collector) throws Exception {
                String[] words = line.split(" ");
                for (int i = 0; i < words.length; i++) {
                    String word = words[i];
                    collector.collect(new WordWithCount(word, 1l));
                }
            }
        });
        UnsortedGrouping<WordWithCount> wordWithCountUnsortedGrouping = flatMapOperator.groupBy(new KeySelector<WordWithCount, Object>() {
            @Override
            public Object getKey(WordWithCount wordWithCount) throws Exception {
                return wordWithCount.word;
            }
        });
        ReduceOperator<WordWithCount> reduce = wordWithCountUnsortedGrouping.reduce(new ReduceFunction<WordWithCount>() {
            @Override
            public WordWithCount reduce(WordWithCount t0, WordWithCount t1) throws Exception {
                return new WordWithCount(t0.word, t0.count + t1.count);
            }
        });
        List<WordWithCount> collect = reduce.collect();

        collect.forEach(System.out::println);
    }

    static class WordWithCount{
        private String word;
        private Long count;


        public WordWithCount(String word, Long count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
