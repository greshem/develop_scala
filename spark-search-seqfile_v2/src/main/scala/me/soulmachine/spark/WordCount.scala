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

case class dpiidcismlog(commandid: String, houseid: String, gathertime: String, srcip: String,
  destip: String, srcport: String, destport: String, domainname: String, proxytype: String,
  proxyip: String, proxyport: String, title: String, content: String, url: String,atname:String)



object WordCount {

        def search_line(file_name:String ,input:org.apache.hadoop.io.BytesWritable): List[ dpiidcismlog]=
        {
                var file=new String(input.getBytes);
                var lines=file.split("\n");
                //var b=lines.map(_.split("<<<!>>>")).filter(p => p.length >= 15).map(p=>(dpiidcismlog(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14)) )).toList;
                var b=lines.map(_.split("<!!@@!!>")).filter(p => p.length >= 15).map(p=>(dpiidcismlog(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14)) )).toList;

                //var c=lines.length;
                //println(file_name+"--->"+c);
                return b;
        }



    def count_line(file_name:String ,input:org.apache.hadoop.io.BytesWritable): Int =
    {
        var file=new String(input.getBytes);
        var lines=file.split("\n");
        var c=lines.length;
        //println(file_name+"--->"+c);
        return c;
    }

  def main(args: Array[String]) {

    //val tempDir = Utils.createTempDir()
    //val tempDir = "/tmp/dsfaslin";


    val conf = new SparkConf().setAppName("seq_int_int")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._


    //val rdd2 = sc.sequenceFile[String, org.apache.hadoop.io.BytesWritable]("hdfs:///xinan_2017_04_27.seq")  
    val rdd2 = sc.sequenceFile[String, org.apache.hadoop.io.BytesWritable]("/root/xinan_2017_04_27.seq")  
    //val rdd2 = sc.sequenceFile[String, org.apache.hadoop.io.BytesWritable]("/root/xinan_2017_05_06_5510.seq")  

    //rdd2.collect().map( each => count_line(each._1, each._2))
    //rdd2.map( each => count_line(each._1, each._2))
    //获取第五个文件
    //var c=rdd2.map(elem=> elem._2.getBytes)
    //var file=new String(c.get(0))
    //var lines=file.split("\n")
    //var d=lines.map(_.split("<<<!>>>")).filter(p => p.length >= 15).map(p => p(15))
    //d.foreach(println);

    sc.parallelize(1 to 8);
    var array=rdd2.flatMap( each => search_line(each._1, each._2)).toDF();
    //array.filter(p=>p.srcip=="80").count
    var value=array.filter(" srcport==80")
    value.write.mode("append").parquet("/tmp/parquet")
    sc.stop()
  }

  def execute(input: String, output: String, master: Option[String] = Some("local")): Unit = {
    val sc = {
      val conf = new SparkConf().setAppName("Spark WordCount")
      for (m <- master) {
        conf.setMaster(m)
      }
      new SparkContext(conf)
    }

    // Adapted from Word Count example on http://spark-project.org/examples/
    val file = sc.textFile(input)
    val words = file.flatMap(line => tokenize(line))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.saveAsTextFile(output)
  }

  // Split a piece of text into individual words.
  private def tokenize(text : String) : Array[String] = {
    // Lowercase each word and remove punctuation.
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }
}
