
package org.apache.spark.greshem

import java.util.Properties

import java.io.File
import scala.io.Source


import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object hdfswordcount {


  private def printUsage(): Unit = {
    val usage: String =
      "Usage: localFile dfsDir\n" +
      "\n" +
      "localFile - (string) local file to use in test\n" +
      "dfsDir - (string) DFS directory for read/write tests\n"

    println(usage)
  }

  def runLocalWordCount(fileContents: List[String]): Int = {
    fileContents.flatMap(_.split("\\s+"))
      .filter(_.nonEmpty)
      .groupBy(w => w)
      .mapValues(_.size)
      .values
      .sum
  }


  def main(args: Array[String]): Unit = 
  {
    if (args.length < 2) {
      printUsage()
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("HdfsFileTest")
    val ctx = new SparkContext(sparkConf)

    val localFilePath = args(0)
    val dfsDirPath = args(1)
    val lineList = Source.fromFile(new File(localFilePath)).getLines().toList

    val localWordCount = runLocalWordCount(lineList)

    ctx.parallelize(lineList).saveAsTextFile(dfsDirPath)

    val dfsWordCount = ctx.textFile(dfsDirPath).
      flatMap(_.split("\\s+")).
      filter(_.nonEmpty).
      map(w => (w, 1)).
      countByKey().
      values.
      sum

    ctx.stop()

    if (localWordCount == dfsWordCount) {
      println(s"Success! Local Word Count ($localWordCount) " +
        s"and DFS Word Count ($dfsWordCount) agree.")
    } else {
      println(s"Failure! Local Word Count ($localWordCount) " +
        s"and DFS Word Count ($dfsWordCount) disagree.")
    }
  }
}
