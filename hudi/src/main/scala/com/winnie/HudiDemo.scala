package com.winnie

import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.{DataSourceWriteOptions, QuickstartUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object HudiDemo {
  val sparkConf = new SparkConf()
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.default.parallelism", "9")
    .set("spark.sql.shuffle.partitions", "9")
  // 创建spark客户端
  val spark = SparkSession.builder()
    .config(sparkConf)
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
    val tableName = "hudi_cow_table"
    val basePath = "/Volumes/disk/yyq/data/hudi/"
//  val tableName = "t_user"
//  val basePath = "/Volumes/disk/yyq/data/kafkahudi/"


  def main(args: Array[String]): Unit = {

          insert(spark,tableName,basePath)
    //      upsert(spark,tableName,basePath)
//    delete(spark, tableName, basePath)
    find(spark, basePath)
  }


  /**
   * 查询
   *
   * @param spark
   * @param basePath
   */
  def find(spark: SparkSession, basePath: String): Unit = {
    val df = spark.read.format("org.apache.hudi")
      .load(basePath + "/*")
    df.show()
    //    val tableNameView = tableName + "_tmp"
    //    df.createOrReplaceTempView(tableNameView)
    //    val resultDF = spark.sql(s"select id from ${tableNameView}")
    //    resultDF.show()


  }

  /**
   * 插入方式：
   * Overwrite
   * 1. delete basePath's hoodie table
   * 2. create new hoodie table
   * 3. craete one parquet per partition
   *
   * @param spark
   * @param tableName
   * @param basePath
   */
  def insert(spark: SparkSession, tableName: String, basePath: String): Unit = {
    val df = spark.createDataFrame(Seq(
      (1, "modifyFirstName2", java.sql.Date.valueOf("2010-01-01")), // 同一个recordkey， 只记录第一个
      (1, "modifyFirstName", java.sql.Date.valueOf("2010-01-01")),
      (1, "eatom", java.sql.Date.valueOf("2010-01-01")),
      (2, "modifyFirstName", java.sql.Date.valueOf("2010-01-01")),
      (3, "Second Value", java.sql.Date.valueOf("2010-02-01"))
    )).toDF("id", "name", "ts")

    df.write.format("org.apache.hudi").
      options(QuickstartUtils.getQuickstartWriteConfigs()).
      option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "id").
      option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "ts").
      option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://testcdh001:10000")
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, true)
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "winnie")
//      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, tableName)
      .option(HoodieWriteConfig.TABLE_NAME, tableName).
      mode(SaveMode.Overwrite).
      save(basePath)


  }

  /**
   * 插入并且更新数据，recordkey作为唯一主键
   * SaveMode.Append
   * 1. if recordkey already exists, it would be recovered
   * 2. it will append a new parquet per partition
   *
   * @param spark
   * @param tableName
   * @param basePath
   */
  def upsert(spark: SparkSession, tableName: String, basePath: String): Unit = {
    val df = spark.createDataFrame(Seq(
      (4, "lucy", java.sql.Date.valueOf("2010-01-02")),
      (5, "tom-lucy", java.sql.Date.valueOf("2010-02-01")),
      (6, "tom", java.sql.Date.valueOf("2010-02-01"))
    )).toDF("id", "name", "ts")

    df.write.format("org.apache.hudi").
      options(QuickstartUtils.getQuickstartWriteConfigs()).
      option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "id").
      option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "ts").
      option(HoodieWriteConfig.TABLE_NAME, tableName).
      mode(SaveMode.Append).
      save(basePath);
  }

  /**
   * delete hoodie record
   * recordkey must required
   * partitinopath
   *
   * @param spark
   * @param tableName
   * @param bashPath
   */
  def delete(spark: SparkSession, tableName: String, bashPath: String): Unit = {
    //    val df = spark.createDataFrame(Seq(      (1, java.sql.Date.valueOf("2010-01-01"))    )).toDF("id","ts")
    val df = spark.createDataFrame(Seq((5, "1599100107164"))).toDF("id", "ts")

    df.write.format("org.apache.hudi").
      options(QuickstartUtils.getQuickstartWriteConfigs()).
      //      option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "id").
      //      option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "ts").
      option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL). // 删除
      option(HoodieWriteConfig.TABLE_NAME, tableName).
      mode(SaveMode.Append).
      save(basePath);
  }
}
