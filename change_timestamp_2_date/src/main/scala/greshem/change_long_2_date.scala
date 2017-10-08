
package org.apache.spark.greshem

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import java.text.SimpleDateFormat

// 1469930200
object change_long_2_date {
  def main(args: Array[String]): Unit = 
  {
    if (args.length != 2) {
      System.err.println("Usage: 0    timestamp       ")
      System.err.println("Usage: 1    2017-04-01       ")
      System.exit(1)
    }

    val cmd = args(0)
    //val table = args(1)
    //val user = args(2)
    //val passwd = args(3)

//    val conf = new SparkConf().setAppName("change_long_2_date")
 //   val sc = new SparkContext(conf)
  //  val sqlContext = new SQLContext(sc)
   // import sqlContext.implicits._


    if  (cmd.toInt == 0)
    {
      println("#timestamp 2 time_string ===============================");
      //var  startTime="1469930200";
      var  startTime=args(1);
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val  s_date:String = sdf.format(startTime.toLong*1000)
      println("raw: "+startTime);
      println("yyyy-MM-dd:"+s_date);
    }
    else
    {
      println("#time_string 2 timestamp ===============================");
      //2016-07-31
      //var sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      //var sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      //var time_str="2017-06-01";
      var time_str=args(1);
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      var  time=sdf.parse(time_str).getTime();;
      println("string:"+time_str);
      println("time:"+time/1000);
    }
    //sc.stop()
  }
}
