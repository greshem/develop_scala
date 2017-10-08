
package org.apache.spark.greshem

import java.util.Properties
import java.io.File

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

sealed  case class Weather(date: String, city: String, minTem: Int, maxTem: Int)

object DataFrameTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) 
    {
      System.err.println("Usage: <input-file> <output-dir>")
      System.exit(1)
    }

    val inputFile = "/root/bin_ext/spark/SparkDemo/src/main/resources/weather.txt" 

    val outputDir = args(1)

    val conf = new SparkConf().setAppName("DataFrameTest")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._


    val df = sc.textFile(inputFile).map(_.split( """\s+""")).map(p => Weather(p(0), p(1), p(2).toInt, p(3).toInt)).toDF

    // print schema
    df.printSchema()
    // number of rows
    println("Count: " + df.count())

    // first row
    println("First row: " + df.first())

    // Displays the top 20 rows
    df.show()

    println("Filter city beijing")
    df.filter(df("city") === "cc").show()
    df.filter(df("city") === "北京").show()
    df.filter("city = '北京' ").show()

    println("Group by city")
    df.groupBy("city").count().show()

    println("Group by city, avg minTem")
    df.groupBy("city").avg("minTem").show()

    df.registerTempTable("weather")

    println("Group by city, avg minTem throw sql")
    sqlContext.sql("SELECT city, avg(minTem) FROM weather group by city").show()

    // 注意是写到了 json 目录中，而非是单个文件
    println("Write to: " + outputDir + File.separator + "json")


    var num=(new util.Random).nextInt(1000)
    df.write.mode(SaveMode.Overwrite).json(outputDir + File.separator + "json")

    println("/tmp/test_"+num)
    df.write.mode(SaveMode.Overwrite).parquet("/tmp/test_"+num)

    sc.stop()
  }
}
