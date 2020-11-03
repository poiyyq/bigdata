package com.winnie;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.QuickstartUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.IOException;
import java.util.*;

public class SparkStreamingKafkaHudiDemo {
    public static void main(String[] args) throws InterruptedException, IOException {

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "testcdh002:9092,testcdh003:9092,testcdh005:9092");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "testGroup");
        SparkConf sparkConf = new SparkConf()
                .setAppName(SparkStreamingKafkaHudiDemo.class.getName())
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        SparkSession spark = SparkSession.builder().config(sparkConf).master("local[2]").getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Duration.apply(60000));
        List<String> topics = Arrays.asList("example");
        // 得到kafka数据流
        JavaInputDStream<ConsumerRecord<Object, Object>> stream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferBrokers(), ConsumerStrategies.Subscribe(topics, kafkaParams));

        JavaDStream<String> javaDStream = stream.map(record -> {
            String json = String.valueOf(record.value());
            return json;
        }).filter(recordJson -> {
            JSONObject jsonObject = JSON.parseObject(recordJson);
            String database = String.valueOf(jsonObject.get("database"));
            if (StringUtils.isNotBlank(database) && "winnie".equals(database)) {
                // 这里的数据是我想要的数据
                return true;
            }
            return false;
        });
        String basePath = "/Volumes/disk/yyq/data/kafkahudi/";

        javaDStream.foreachRDD(rdd -> {
            List<String> collect = rdd.collect();
            collect.forEach(rddJson -> {
                JSONObject jsonObject = JSON.parseObject(rddJson);
                String tableName = String.valueOf(jsonObject.get("table")); // 表名
                String database = String.valueOf(jsonObject.get("database")); // 数据库
                String type = String.valueOf(jsonObject.get("type")); // 增删改查
                String ts = String.valueOf(jsonObject.get("ts")); //  时间戳
                List<String> pkNames = (List<String>) jsonObject.get("pkNames");   // 当前测试唯一主键， 如果联合主键需要另外考虑
                String idkey = pkNames.get(0);
                JSONArray datas = (JSONArray) (jsonObject.get("data"));
                // 组装rows
                List<Row> rows = getRows(datas, ts);
                // 配置StructType
                StructType schema = getStructType(datas);
                Dataset<Row> df = spark.createDataFrame(rows, schema);
                switch (type) {
                    case "UPDATE":
                    case "INSERT":
                        df.write().format("org.apache.hudi")
                                .options(QuickstartUtils.getQuickstartWriteConfigs())
                                .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "ts")   // 一般取时间字段，当两个记录相同时，取最大的一个
                                .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), idkey)
//                                .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "ts") // 不适合配置分区，建议使用默认default
                                .option(DataSourceWriteOptions.OPERATION_OPT_KEY(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL())
                                .option(HoodieWriteConfig.TABLE_NAME, tableName)
                                // hive配置
                                .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY(), database)
                                .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY(), tableName)
                                .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY(), "jdbc:hive2://testcdh001:10000")
                                .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY(), true)
                                .mode(SaveMode.Append)
                                .save(basePath);
                        break;
                    case "DELETE":
                        df.write().format("org.apache.hudi")
                                .options(QuickstartUtils.getQuickstartWriteConfigs())
//                                .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "ts")   // 一般取时间字段，当两个记录相同时，取最大的一个
                                .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), idkey)
//                                .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "ts") // 不适合配置分区，建议使用默认default
                                .option(DataSourceWriteOptions.OPERATION_OPT_KEY(), DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL())
                                .option(HoodieWriteConfig.TABLE_NAME, tableName)
                                // hive配置
                                .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY(), database)
                                .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY(), tableName)
                                .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY(), "jdbc:hive2://testcdh001:10000")
                                .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY(), true)
                                .mode(SaveMode.Append)
                                .save(basePath);
                        break;
                    default:
                }
            });
        });


        jssc.start();
        jssc.awaitTermination();
    }

    /**
     * 组装rows， 用于创建dataframe
     *
     * @param datas
     * @param ts
     * @return
     */
    private static List<Row> getRows(JSONArray datas, String ts) {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < datas.size(); i++) {
            List<String> list = new ArrayList<>();
            JSONObject dataJsonObject = (JSONObject) datas.get(i);
            Set<String> keySet = dataJsonObject.keySet();
            Iterator<String> iterator = keySet.iterator();
            while (iterator.hasNext()) {
                String key = iterator.next();
                list.add(String.valueOf(dataJsonObject.get(key)));
            }
            if (!list.contains("ts")) {
                list.add(ts);
            }
            Row row = RowFactory.create(list.toArray());
            rows.add(row);
        }
        return rows;
    }

    /**
     * 获取schema模板
     *
     * @param datas
     * @return
     */
    private static StructType getStructType(JSONArray datas) {
        List<StructField> schemaFields = new ArrayList<StructField>();
        JSONObject data = (JSONObject) datas.get(0);
        Set<String> keys = data.keySet();
        keys.forEach(key -> {
            schemaFields.add(DataTypes.createStructField(key, DataTypes.StringType, true));
        });
        schemaFields.add(DataTypes.createStructField("ts", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(schemaFields);
        return schema;
    }
}
