package org.apache.spark.greshem


import java.io.File
import java.nio.charset.Charset

//import org.apache.spark.SparkConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.google.common.io.Files

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


    val conf = new SparkConf().setAppName("HdfsWordCount")
    //val sc = new SparkContext(conf)

    // Create the context
    val ssc  =  new StreamingContext(conf, Seconds(1))
    var number=1000;
    var count=ssc.sparkContext//.broadcast(0)
       val outputFile = new File("output.xml")

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream(args(0))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    //wordCounts.print()

      wordCounts.foreachRDD{ (rdd, time) => 
      //var tmp= count.value +1 ;
      //println("FFFFFFFF="+tmp)
      //count.unpersist()
      //count = ssc.sparkContext.broadcast(tmp);
      //val tmp=100;
      //println("############################## loop count="+tmp +"|"+time)

      if (rdd.count>0)
        {rdd.foreach(println);
    val counts = "Counts at time " + time + " " + rdd.collect().mkString("[", ", ", "]")
    println(counts)
    println("Appending to ")
    Files.append(counts + "\n", outputFile, Charset.defaultCharset())


        }
    } ;
    //println("GGGGGGGGGGGGGGGGGGG"+count);

    ssc.start()
    ssc.awaitTermination()
  }
}
