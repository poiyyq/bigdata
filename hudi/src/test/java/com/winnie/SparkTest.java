package com.winnie;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import scala.collection.generic.SeqFactory;

import java.util.*;

public class SparkTest {
    @Test
    public void test1(){
        SparkConf sparkConf = new SparkConf()
                .setAppName(SparkTest.class.getName())
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        SparkSession spark = SparkSession.builder().config(sparkConf).master("local[2]").getOrCreate();


        List<Row> rows = new ArrayList<>();
        Row row = RowFactory.create("2","lucy");
        rows.add(row);
        Row row1 = RowFactory.create("3","lucy1");
        rows.add(row1);
        List<StructField> schemaFields = new ArrayList<StructField>();
        schemaFields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(schemaFields);

        Dataset<Row> df = spark.createDataFrame(rows, schema);
        df.show();


    }


    /**
     * 获取schema模板
     * @param datas
     * @return
     */
    private static StructType getStructType(List<Map<String, String>> datas) {
        List<StructField> schemaFields = new ArrayList<StructField>();
        Map<String, String> data = datas.get(0);
        Set<String> keys = data.keySet();
        boolean tsExists = false;
        Iterator<String> iterator = keys.iterator();
        while(iterator.hasNext()){
            String key = iterator.next();
            if(key.equals("ts")){
                tsExists = true;
            }
            schemaFields.add(DataTypes.createStructField(key, DataTypes.StringType, true));
        }
        if(!tsExists){
            schemaFields.add(DataTypes.createStructField("ts", DataTypes.StringType, true));
        }
        StructType schema = DataTypes.createStructType(schemaFields);
        return schema;
    }
}
