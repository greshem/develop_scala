package com.zhongying.dpi.dpiidcismlog.query

import java.text.SimpleDateFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer

case class dpiidcismlog(commandid: String, houseid: String, gathertime: String, srcip: String,
  destip: String, srcport: String, destport: String, domainname: String, proxytype: String,
  proxyip: String, proxyport: String, title: String, content: String, url: String,atname:String)
object DPIIDCISMLogQuery {

  def main(args: Array[String]) {
    try {
      if (args.length != 4) {
        System.exit(2)
      } else {
        println("QueryParams: " + args(0) + ", input: " + args(1) + ", Output: " + args(2) + ",oracleParams: " + args(3))
      }
      
      val input:String = args(1)
      val output:String = args(2)
      val outputtemp:String = output+"temp"
      val outputfinal:String = output+"result"
      val params: Array[String] = args(0).split(",")
      val sys: String = params(0).substring(params(0).indexOf("=")+1)
      val startTime:String = params(4).substring(params(4).indexOf("=")+1)
      val endTime:String = params(5).substring(params(5).indexOf("=")+1)
      val commandid: String = params(2).substring(params(2).indexOf("=")+1)
      val houseId: String = params(3).substring(params(3).indexOf("=")+1)
      val srcip: String = params(6).substring(params(6).indexOf("=")+1)
      val srcport: String = params(7).substring(params(7).indexOf("=")+1)
      val destip: String = params(8).substring(params(8).indexOf("=")+1)
      val destport: String = params(9).substring(params(9).indexOf("=")+1)
      val domainname: String = params(10).substring(params(10).indexOf("=")+1)
      
      println("startTime="+startTime+",endTime="+endTime)
      val filePath = dealFileByTime(input,startTime, endTime)
      val files = ArrayBuffer[String]()
      if(filePath != null && filePath.length>0){
        val conf:Configuration = new Configuration()
        val filesys:FileSystem = FileSystem.get(conf)
        for (x <- filePath){
            if(filesys.exists(new Path(x))){
              files += x+"*";
            }
        }
      }
      
      if(files != null && files.length>0){
        println("files length:"+files.length)
        val sparkConf = new SparkConf().setAppName("DPIIDCISMLogQuery")
        val sc = new SparkContext(sparkConf)
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)

        import sqlContext.implicits._
        
        var strfilter:String = ""
        if (houseId != null && !"".equals(houseId.trim())) {
            if(strfilter != null && !"".equals(strfilter)){
              strfilter = strfilter + " and houseid = '" + houseId.trim() + "'"
            }else{
              strfilter = "houseid = '" + houseId.trim() + "'"
            }
        }
        if (srcip != null && !"".equals(srcip.trim())) {
            if(strfilter != null && !"".equals(strfilter)){
              strfilter = strfilter + " and srcip like '%" + srcip.trim() + "%'"
            }else{
              strfilter = "srcip like '%" + srcip.trim() + "%'"
            }
        }
        if (srcport != null && !"".equals(srcport.trim())) {
            if(strfilter != null && !"".equals(strfilter)){
              strfilter = strfilter + " and srcport = '" + srcport.trim() + "'"
            }else{
              strfilter = "srcport = '" + srcport.trim() + "'"
            }
        }
        if (destip != null && !"".equals(destip.trim())) {
            if(strfilter != null && !"".equals(strfilter)){
              strfilter = strfilter + " and destip like '%" + destip.trim() + "%'"
            }else{
              strfilter = "destip like '%" + destip.trim() + "%'"
            }
        }
        if (destport != null && !"".equals(destport.trim())) {
            if(strfilter != null && !"".equals(strfilter)){
              strfilter = strfilter + " and destport = '" + destport.trim() + "'"
            }else{
              strfilter = "destport = '" + destport.trim() + "'"
            }
        }
        if (domainname != null && !"".equals(domainname.trim())) {
            if(strfilter != null && !"".equals(strfilter)){
              strfilter = strfilter + " and domainname like '%" + domainname.trim() + "%'"
            }else{
              strfilter = "domainname like '%" + domainname.trim() + "%'"
            }
        }
        if (commandid != null && !"".equals(commandid.trim())) {
            if(strfilter != null && !"".equals(strfilter)){
              strfilter = strfilter + " and commandid = '" + commandid.trim() + "'"
            }else{
             strfilter = "commandid = '" + commandid.trim() + "'"
            }
        }
        if (startTime != null && !"".equals(startTime.trim())) {
            if(strfilter != null && !"".equals(strfilter)){
              strfilter = strfilter + " and gathertime >= " + startTime.trim()
            }else{
              strfilter = "gathertime >= " + startTime.trim()
            }
        }
        if (endTime != null && !"".equals(endTime.trim())) {
            if(strfilter != null && !"".equals(strfilter)){
              strfilter = strfilter + " and gathertime <= " + endTime.trim()
            }else{
              strfilter = "gathertime <= " + endTime.trim()
            }
        }
      
        for (x <- files){
          val value = sc.textFile(x).map(_.split("\\<\\!\\!\\@\\@\\!\\!\\>")).filter(p => p.length >= 15).map(p => dpiidcismlog(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14))).toDF()
          
          if (strfilter != null && !"".equals(strfilter)){
              value.filter(strfilter).write.mode("append").parquet(outputtemp)
          }else{
              value.write.mode("append").parquet(outputtemp)
          }
    
        }

        val newdf = sqlContext.read.parquet(outputtemp)
        newdf.registerTempTable("dpiidcismlog")
      
        val oracleparams: Array[String] = args(3).split(",")
        val driver = "oracle.jdbc.driver.OracleDriver"
        val url = oracleparams(0)
        val username = oracleparams(1)
        val password = oracleparams(2)
      
        println("url: " + url + ", username: " + username + ", password: " + password)

        val readerhouse = sqlContext.read.format("jdbc")
        readerhouse.option("url", url)
        readerhouse.option("dbtable", "dpihouseinfo")
        readerhouse.option("driver", driver)
        readerhouse.option("user", username)
        readerhouse.option("password", password)
        readerhouse.load().registerTempTable("dpihouseinfo")
      
        val readercmd = sqlContext.read.format("jdbc")
        readercmd.option("url", url)
        readercmd.option("dbtable", "DPIISMISafeCmd")
        readercmd.option("driver", driver)
        readercmd.option("user", username)
        readercmd.option("password", password)
        readercmd.load().registerTempTable("DPIISMISafeCmd")
        if(sys.equals("city")){
        
        }else{
          val readermessage = sqlContext.read.format("jdbc")
          readermessage.option("url", url)
          readermessage.option("dbtable", "dpicurmessage")
          readermessage.option("driver", driver)
          readermessage.option("user", username)
          readermessage.option("password", password)
          readermessage.load().registerTempTable("dpicurmessage")
        }
        
        val readerSingleUserNodeauth = sqlContext.read.format("jdbc")
         readerSingleUserNodeauth.option("url", url)
         readerSingleUserNodeauth.option("dbtable", "SingleUserNodeauth")
         readerSingleUserNodeauth.option("driver", driver)
         readerSingleUserNodeauth.option("user", username)
         readerSingleUserNodeauth.option("password", password)
         readerSingleUserNodeauth.load().registerTempTable("SingleUserNodeauth")

        val sqlstr: String = getSQLStr(params);
        println("sqlstr:"+sqlstr)
        sqlContext.sql(sqlstr).rdd.saveAsTextFile(outputfinal, classOf[GzipCodec])

        sc.stop()
        System.exit(0)
      }else{
          System.exit(1)
      }
      
    } catch {
      case e: Exception => println("exception:"+e.getMessage); System.exit(-1)
    }

  }

  def getSQLStr(params: Array[String]): String = {
    var sqlstr: String = ""
    val sys: String = params(0).substring(params(0).indexOf("=")+1)
    val cmdtype: String = params(1).substring(params(1).indexOf("=")+1)
    val houseId: String = params(3).substring(params(3).indexOf("=")+1)
    val netUserID: String = params(11).substring(params(11).indexOf("=")+1)

    if (sys.equals("city")) {
      sqlstr = "select m.CMDTYPE,l.commandid,h.HOUSENAME,l.gathertime,l.srcip,l.srcport,l.destip,l.destport,l.domainname," +
        "l.proxytype,l.proxyip,l.proxyport,l.title,l.url,l.atname,h.IDCID,m.INTERCOMMANDID,'end' from dpiidcismlog l left outer join DPIISMISafeCmd m on l.commandid=m.COMMANDID, dpihouseinfo h,SingleUserNodeauth s " +
        "where m.COMMANDID is not null and l.houseid = h.HOUSEID and m.ISVISIBLE = 1 and h.NODECODE=s.NODECODE and s.NETUSERID = '"+netUserID+"' and s.AUTHTYPE = 'CFG' "
        
    } else {
      sqlstr = "select d2.PARAVALUE,l.commandid,h.HOUSENAME,l.gathertime,l.srcip,l.srcport,l.destip,l.destport,l.domainname," +
        "l.proxytype,l.proxyip,l.proxyport,l.title,l.url,l.atname,h.IDCID,'end' from dpiidcismlog l left outer join DPIISMISafeCmd m on l.commandid=m.COMMANDID," +
        "dpicurmessage d1,dpicurmessage d2,dpihouseinfo h,SingleUserNodeauth s  where l.houseid = h.HOUSEID and m.COMMANDID is null and h.NODECODE=s.NODECODE and s.NETUSERID = '"+netUserID+"' and s.AUTHTYPE = 'CFG' " +
        " and l.commandid = d1.PARAVALUE  and d1.MESSAGETYPE = 16 and d1.PARACODE = 'CommandID' and d1.MESSAGENO = d2.MESSAGENO and d2.MESSAGETYPE = 16 " +
        " and d2.PARACODE = 'CMDType'"
      
      if (cmdtype != null && !"".equals(cmdtype.trim())) {
        sqlstr = sqlstr + " and d2.PARAVALUE='" + cmdtype.trim() + "' "
      }
    }
    
    if (houseId != null && !"".equals(houseId.trim())){
          
    }else{
         // sqlstr = sqlstr + " and  exists (select s.nodecode from SingleUserNodeauth s where s.nodecode=h.nodecode and s.netuserid = '"+netUserID+"' and s.authtype = 'CFG') "
    }

    if (cmdtype != null && !"".equals(cmdtype.trim())) {
      if (sys.equals("city")) {
        sqlstr = sqlstr + " and m.CMDTYPE = '" + cmdtype.trim() + "'"
      } else {

      }

    }

    sqlstr
  }
  
  def dealFileByTime(input:String,startTime:String,endTime:String):ArrayBuffer[String] = {
    val filePath = ArrayBuffer[String]()
    
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

}
