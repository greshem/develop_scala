
package org.apache.spark.greshem

import java.util.Properties
import java.io.File

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

sealed  case class Weather(date: String, city: String, minTem: Int, maxTem: Int)

case class dpiidcismlog(commandid: String, houseid: String, gathertime: String, srcip: String,
  destip: String, srcport: String, destport: String, domainname: String, proxytype: String,
  proxyip: String, proxyport: String, title: String, content: String, url: String,atname:String)


object DataFrameTest {
  def main(args: Array[String]): Unit = {


    val inputFile = "/root/bin_ext/zhongying_scala/data.txt" 


    var num=(new util.Random).nextInt(1000)
    val outputDir = "/tmp/zhongying_output_append/"

    val conf = new SparkConf().setAppName("DataFrameTest")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._


    //val df = sc.textFile(inputFile).map(_.split( """\s+""")).map(p => Weather(p(0), p(1), p(2).toInt, p(3).toInt)).toDF

    val df = sc.textFile(inputFile).map(_.split("<<<!>>>")).filter(p => p.length >= 15).map(p => dpiidcismlog(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14))).toDF()

    // print schema
    df.printSchema()
    // number of rows
    println("Count: " + df.count())

    // first row
    println("First row: " + df.first())

    // Displays the top 20 rows
    df.show()

    println("Filter city beijing")
    //df.filter(df("city") === "cc").show()
    //df.filter(df("city") === "北京").show()

    println("Group by city")
    //df.groupBy("city").count().show()

    println("Group by city, avg minTem")
    //df.groupBy("city").avg("minTem").show()

    //df.registerTempTable("weather")
    //println("Group by city, avg minTem throw sql")
    //sqlContext.sql("SELECT city, avg(minTem) FROM weather group by city").show()

    // 注意是写到了 json 目录中，而非是单个文件
    println("Write to: " + outputDir + File.separator + "json")

    //df.write.mode(SaveMode.Overwrite).json(outputDir + File.separator + "json")
    //df.write.mode(SaveMode.Overwrite).json(outputDir + File.separator + "json")

    println("#OUTPUT: "+outputDir)
    df.write.mode(SaveMode.Append).parquet(outputDir)

    sc.stop()
  }
}
