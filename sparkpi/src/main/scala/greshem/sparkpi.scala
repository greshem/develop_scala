
package org.apache.spark.greshem

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.random


object sparkpi {
  def main(args: Array[String]): Unit = 
  {
    val conf = new SparkConf().setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2

    // slices 对应于 partition 个数，平均每个 partition 有 100000L 个元素
    val n = math.min(10000000L * slices, Int.MaxValue).toInt // avoid overflow

    // 这里可以理解为一个循环，每个 partition 上循环次数为 100000L
    val count = spark.parallelize(1 until n, slices).map { i =>
        val x = random * 2 - 1 // random return double value between [0.0, 1.0], so random * 2 - 1 return value between [-1.0, 1.0]
        val y = random * 2 - 1

        if (x * x + y * y < 1) 1 else 0
      }.reduce(_ + _)

    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()

  }
}
