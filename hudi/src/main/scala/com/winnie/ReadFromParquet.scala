package com.winnie

import org.apache.spark.sql.SparkSession

object ReadFromParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("ReadFromParquet").getOrCreate()
//    val df = spark.read.parquet("/Volumes/disk/yyq/data/hudi/*/*.parquet")
    val df = spark.read.parquet("/Volumes/disk/yyq/data/hudi/2010-02-01/*837.parquet")
    df.show()
  }
}
