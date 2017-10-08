
package org.apache.spark.greshem

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object wordcount {
  def main(args: Array[String]): Unit = 
  {
    if (args.length != 2) 
    {
      System.err.println("Usage: SparkWordCount <input> <output>")
    }
    val conf = new SparkConf().setAppName("Spark WordCount")
    val sc = new SparkContext(conf)

    val file = sc.textFile(args(0))
    val counts = file.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(args(1))

    sc.stop()

  }
}
