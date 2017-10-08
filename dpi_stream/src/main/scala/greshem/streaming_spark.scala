package org.apache.spark.greshem


import java.text.SimpleDateFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer


import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class dpiidcismlog(commandid: String, houseid: String, gathertime: String, srcip: String,
  destip: String, srcport: String, destport: String, domainname: String, proxytype: String,
  proxyip: String, proxyport: String, title: String, content: String, url: String,atname:String)


/*
 * 统计指定目录下的 words 数目，注意会对新的 text files 进行统计.
 * 文件放到目录中需要是采用 mv, rename 等原子方式进行的
 */
object streaming_spark {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: <directory>")
      System.exit(1)
    }

  val updateFunc = (values: Seq[dpiidcismlog], state: Option[dpiidcismlog]) => { //StateFul需要定义的处理函数，第一个参数是本次进来的值，第二个是过去处理后保存的值  
      val currentCount = values(0).commandid//求和  
      val previousCount = state.getOrElse("")// 如果过去没有 即取0  
      Some(currentCount + previousCount)// 求和  
  }  

  
  val updateFunc2 = (values: Seq[String], state: Option[String]) => { //StateFul需要定义的处理函数，第一个参数是本次进来的值，第二个是过去处理后保存的值  
      val currentCount = values(0) //求和  
      val previousCount = state.getOrElse("")// 如果过去没有 即取0  
      Some(currentCount + previousCount)// 求和  
  }  

    val sparkConf = new SparkConf().setAppName("HdfsWordCount")

    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream(args(0))

    //val b=lines.map(_.split("<!!@@!!>")).filter(p => p.length >= 15).map(p=>(dpiidcismlog(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14)
    val b=lines.map(_.split("<<<!>>>")).filter(p => p.length >= 15).map(p=>(p(0), dpiidcismlog(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14)
) )).filter(p=>p._1=="3724").updateStateByKey[String](updateFunc2);
    
    //val stateDstream = wordCounts.updateStateByKey[Int](updateFunc)//调用处理函数 updateFunc  

    //val words = lines.flatMap(_.split(" "))
    //val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    //println(wordCounts.count)
    b.print();

    //wordCounts.saveAsTextFile("/tmp/save9999/");


    ssc.start()
    ssc.awaitTermination()
  }
}
