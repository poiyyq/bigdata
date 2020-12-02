package com.winnie.scala


import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._

object SinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val path = "file:///Users/admin/tmp/sink"
    val stream = env.readTextFile("/Users/admin/code/bigdata/flink/src/main/resources/worldcount.txt")
//    stream.writeAsText(path,FileSystem.WriteMode.OVERWRITE).setParallelism(2)
    val tuple = stream.map(
      t => Tuple1(1, 2, 3)
    )
    tuple.writeAsCsv(path,FileSystem.WriteMode.OVERWRITE,"/n",",").setParallelism(2)
    env.execute()
  }
}
