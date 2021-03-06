
package org.apache.spark.greshem

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object gen_huge_file {
  def uuid = java.util.UUID.randomUUID.toString

  def main(args: Array[String]): Unit = 
  {
    if (args.length < 2) {
      System.err.println("Usage: <partition> <output-dir>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("WordCountTest")
    val sc = new SparkContext(sparkConf)

    val partition = args(0) toInt
    val outputDir = args(1)

    // 生成大约 1G 的数据，目的就是让单词不重复，这样 map-reduce 端无法进行 combine
    // 注意 reduceByKey 是会在 mapper 端进行 merge 的，类似于 mapreduce 中的 combiner 做的事情
    val wc = sc.parallelize(1 to 30000000, partition).map(_ => (uuid, 1)).reduceByKey(_ + _)

    //wc.saveAsTextFile(outputDir)
    wc.saveAsSequenceFile(outputDir)

    sc.stop()


  }
}
