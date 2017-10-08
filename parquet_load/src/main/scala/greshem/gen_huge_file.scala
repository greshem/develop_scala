
package org.apache.spark.greshem

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object gen_huge_file {
  def uuid = java.util.UUID.randomUUID.toString

  def main(args: Array[String]): Unit = 
  {

    val sparkConf = new SparkConf().setAppName("WordCountTest")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._




    val newdf = sqlContext.read.parquet("/tmp/zhongying_output_append/")
    println(newdf.count());

    sc.stop()
  }
}
