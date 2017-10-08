package me.soulmachine.spark

import org.apache.spark._
import org.apache.spark.util.Utils


import java.io.{File, FileWriter}

import org.apache.spark.input.PortableDataStream
import org.apache.spark.storage.StorageLevel

import scala.io.Source

import org.apache.hadoop.io._
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.hadoop.mapred.{JobConf, FileAlreadyExistsException, FileSplit, TextInputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileSplit => NewFileSplit, TextInputFormat => NewTextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat => NewTextOutputFormat}



object WordCount {
  def main(args: Array[String]) {

    //val tempDir = Utils.createTempDir()
    val tempDir = "/tmp/dsfaslin";

    val sc = new SparkContext("local", "test")
    val normalDir = new File(tempDir, "output_normal").getAbsolutePath
    val compressedOutputDir = new File(tempDir, "output_compressed").getAbsolutePath
    val codec = new DefaultCodec()

    val data = sc.parallelize("a" * 10000, 1)
    data.saveAsTextFile(normalDir)
    data.saveAsTextFile(compressedOutputDir, classOf[DefaultCodec])

    val normalFile = new File(normalDir, "part-00000")
    val normalContent = sc.textFile(normalDir).collect
    //assert(normalContent === Array.fill(10000)("a"))

    val compressedFile = new File(compressedOutputDir, "part-00000" + codec.getDefaultExtension)
    val compressedContent = sc.textFile(compressedOutputDir).collect
    //assert(compressedContent === Array.fill(10000)("a"))

    //assert(compressedFile.length < normalFile.length)



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
