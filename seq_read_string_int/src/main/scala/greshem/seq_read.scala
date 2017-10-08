
package org.apache.spark.greshem

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.hadoop.io._
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.hadoop.mapred.{JobConf, FileAlreadyExistsException, FileSplit, TextInputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileSplit => NewFileSplit, TextInputFormat => NewTextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat => NewTextOutputFormat}



object seq_read {
  def main(args: Array[String]): Unit = 
  {
    if (args.length < 1) {
      println("Usage: [inputfile]")
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("ParseSequenceFiles")
    val ctx = new SparkContext(sparkConf)

    val inputFile = args(0)

    val data = ctx.sequenceFile(inputFile, classOf[org.apache.hadoop.io.Text], classOf[IntWritable] ).map {  each => (new String(each._1.getBytes()), each._2.get() )  } 
    //data.collect().foreach(println)
    data.take(100).foreach(println)

    ctx.stop()


  }
}
