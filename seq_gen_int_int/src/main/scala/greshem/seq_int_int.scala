
package org.apache.spark.greshem

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}



object seq_int_int {
  def main(args: Array[String]): Unit = 
  {
    var data = Range(1 ,1000000).map{x=>(x, "Linux"+x)};
    //println(data);

    val conf = new SparkConf().setAppName("seq_int_int")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    sc.parallelize(data).saveAsSequenceFile("/tmp/output.seq")

    sc.stop();
  }
}
