package org.apache.spark.greshem

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

sealed case class Weather(date: String, city: String, minTem: Int, maxTem: Int)

object SaveJDBCTest {
  def main(args: Array[String]): Unit = {
    //if (args.length < 5) {
     // System.err.println("Usage: <conn-url> <file-for-read> <table-for-write> <user> <passwd>")
      //System.exit(1)
    //}

    val url = "jdbc:mysql://localhost:3306/test"
    val file = "/root/bin_ext/spark/SparkDemo/src/main/resources/weather.txt";
    val table = "weatch"
    val user = "root"
    val passwd = "qianqian";

    val conf = new SparkConf().setAppName("SaveJDBCTest")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val df = sc.textFile(file).map(_.split( """\s+""")).map(p => Weather(p(0), p(1), p(2).toInt, p(3).toInt)).toDF

    val props = new Properties()
    props.put("user", user)
    props.put("password", passwd)

    df.write.jdbc(url, table, props)

    sc.stop()
  }
}
