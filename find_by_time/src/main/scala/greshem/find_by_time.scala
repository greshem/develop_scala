
package org.apache.spark.greshem

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat


object find_by_time {
  def main(args: Array[String]): Unit = 
  {
    def dealFileByTime(input:String,startTime:String,endTime:String):ArrayBuffer[String] = { val filePath = ArrayBuffer[String]()
    
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val sdf_d = new SimpleDateFormat("yyyy/MM/dd")
    val sdf_h = new SimpleDateFormat("HH")
    
    val  s_date:String = sdf_d.format(startTime.toLong*1000)
    val s_hour:String = sdf_h.format(startTime.toLong*1000)
    val e_date:String = sdf_d.format(endTime.toLong*1000)
		val e_hour:String = sdf_h.format(endTime.toLong*1000)
		
		val start:Long = sdf.parse(sdf.format(startTime.toLong*1000)).getTime()
		val end:Long = sdf.parse(sdf.format(endTime.toLong*1000)).getTime()
		val day:Long = (end - start)/1000/60/60/24
		
		
		val timelist = ArrayBuffer[String]()
		for (a <- 0 until 24) {
		  var hour:String = a.toString()
		  if(a<10){
		    hour = "0"+ hour 
		  }
		  timelist += hour
		  
		}
    
		if (day == 0){
		  for (i <- 0 until timelist.length){
		    if (s_hour.toInt <= timelist(i).toInt && e_hour.toInt >= timelist(i).toInt){
		      filePath += input + s_date + "/" + timelist(i) + "/" 
		    }
		  }
		}else{
		  for (i <- 0 until timelist.length){
		    if (s_hour.toInt <= timelist(i).toInt){
		      filePath += input + s_date + "/" + timelist(i) + "/" 
		    }
		  }
		  if (day > 1){
		    for (j <- 1 until day.toInt){
		      val date:String = sdf_d.format(start + 1000*60*60*24*j)
		      for (i <- 0 until timelist.length){
		         filePath += input + date + "/" + timelist(i) + "/"
		      }
		    }
		  }
		  val s_hour_t = "0000"
		  for (m <- 0 until timelist.length){
		    if (e_hour.toInt <= timelist(m).toInt){
		      filePath += input + e_date + "/" + timelist(m) + "/"
		    }
		  }
		  
		}
    
    filePath
  }



    var test=dealFileByTime("/tmp/","1469930200","1459930900" );
    test.foreach(println);

  }
}
