package me.soulmachine.spark

import org.apache.spark._
import org.apache.spark.util.Utils


import java.io.{File, FileWriter}

import org.apache.spark.input.PortableDataStream
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

import org.apache.hadoop.io._
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.hadoop.mapred.{JobConf, FileAlreadyExistsException, FileSplit, TextInputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileSplit => NewFileSplit, TextInputFormat => NewTextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat => NewTextOutputFormat}
import org.apache.hadoop.io.compress.GzipCodec


object WordCount  {


def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("seq_count")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    if (args.length != 1) {
                System.err.println("Usage:  seq_file");
                      System.exit(1)
      }


    val rdd2 = sc.sequenceFile[String, org.apache.hadoop.io.BytesWritable](args(0))  

    //sc.parallelize(1 to 8);
    sc.parallelize(1 to 4);
    var rdd=rdd2.map( each => (each._1,1) );
    println(rdd.count());
    rdd.saveAsTextFile("file:///tmp/file_list/")

    sc.stop()
  }

}
