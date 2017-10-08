
package org.apache.spark.greshem

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object gen_huge_file {
  def uuid = java.util.UUID.randomUUID.toString

  def main(args: Array[String]): Unit = 
  {

        if (args.length != 1) {
                System.err.println("Usage:  dir")
                      System.exit(1)
      }

    val sparkConf = new SparkConf().setAppName("para_count")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._


    val newdf = sqlContext.read.parquet(args(0))
    println(newdf.count());

    sc.stop()
  }
}

