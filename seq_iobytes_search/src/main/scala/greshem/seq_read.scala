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


case class dpiidcismlog(commandid: String, houseid: String, gathertime: String, srcip: String,
  destip: String, srcport: String, destport: String, domainname: String, proxytype: String,
  proxyip: String, proxyport: String, title: String, content: String, url: String,atname:String)


object seq_read {
  def main(args: Array[String]): Unit = 
  {
    if (args.length < 1) {
      println("Usage: [inputfile]")
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("ParseSequenceFiles")
    sparkConf.set("mapred.min.split.size", "16777216")
    sparkConf.set("mapreduce.input.fileinputformat.split.minsize", "16777216")

    val ctx = new SparkContext(sparkConf)

    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)
    import sqlContext.implicits._


    val inputFile = args(0)

    ctx.parallelize(1 to 16);
    //val data =  ctx.sequenceFile[String, org.apache.hadoop.io.BytesWritable]("/root/xinan_2017_04_27.seq").map {  each => (new String(each._1.getBytes()), new String(each._2.getBytes) )  } 
    val data =  ctx.sequenceFile[String, org.apache.hadoop.io.BytesWritable](inputFile).map {  each => (new String(each._1.getBytes()), new String(each._2.getBytes) )  } 
    
    //println(data.count());

    //var b=data.map(each=>each._2.split("\n").length)
    //println(b.reduce((x,y)=> x+y))
    println(data.first());

    val data2 =  ctx.sequenceFile[String, org.apache.hadoop.io.BytesWritable](inputFile).flatMap {  
each => 
        {   var   file= new String(each._2.getBytes); 
            var lines=file.split("\n");
            // <<<!>>> or  <!!@@!!>
            var b=lines.map(_.split("<!!@@!!>")).filter(p => p.length >= 15).map(p=>(dpiidcismlog(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14)) )).toList;
            b;
            //lines;
           }  
    }.toDF();
    println(data2.first());
    //var value=data2.filter(" srcip==80")
    data2.filter(" srcport = '80' ").write.mode("append").parquet("/tmp/parquet")
    println(data2.count());

    println( "#########################################");
    ctx.stop()

  }
}

