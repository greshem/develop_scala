
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

import scala.io.Source



def count_line(file_name:String ): Int =
{
    var c=file_name.length;    
    return c;
}

println(count_line("test"));


class  test()
{

      var sys			="";
      var startTime		="";
      var endTime		="";
      var commandid		="";
      var houseId		="";
      var srcip			="";
      var srcport		="";
      var destip		="";
      var destport		="";
      var domainname	="";

    def init(input:String)
    {
       var params: Array[String] =input.split(",");
       sys          = params(0).substring(params(0).indexOf("=")+1)
       startTime    = params(4).substring(params(4).indexOf("=")+1)
       endTime      = params(5).substring(params(5).indexOf("=")+1)
       commandid    = params(2).substring(params(2).indexOf("=")+1)
       houseId      = params(3).substring(params(3).indexOf("=")+1)
       srcip        = params(6).substring(params(6).indexOf("=")+1)
       srcport      = params(7).substring(params(7).indexOf("=")+1)
       destip       = params(8).substring(params(8).indexOf("=")+1)
       destport     = params(9).substring(params(9).indexOf("=")+1)
       domainname   = params(10).substring(params(10).indexOf("=")+1)
    
    }

    //val search_str:String= "sys=city,cmdtype=,commandid=11,houseId=237,starttime=1459930200,endtime=1459930400,srcip=,srcport=,destip=,destport=,domainname=";
    def get_filter_str(input:String)
    {
       var params: Array[String] =input.split(",");
       sys          = params(0).substring(params(0).indexOf("=")+1)
       startTime    = params(4).substring(params(4).indexOf("=")+1)
       endTime      = params(5).substring(params(5).indexOf("=")+1)
       commandid    = params(2).substring(params(2).indexOf("=")+1)
       houseId      = params(3).substring(params(3).indexOf("=")+1)
       srcip        = params(6).substring(params(6).indexOf("=")+1)
       srcport      = params(7).substring(params(7).indexOf("=")+1)
       destip       = params(8).substring(params(8).indexOf("=")+1)
       destport     = params(9).substring(params(9).indexOf("=")+1)
       domainname   = params(10).substring(params(10).indexOf("=")+1)

        //生成查询 过滤字符串
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

        println("FILTER_str:  "+strfilter);
    }
}

//val search_str:String= "sys=city,cmdtype=,commandid=11,houseId=237,starttime=1459930200,endtime=1459930400,srcip=,srcport=,destip=,destport=,domainname=";
//para.init(search_str);
//para.get_filter_str();

if (args.length != 4) {
    System.exit(2)
} else {
    println("QueryParams: " + args(0) + ", input: " + args(1) + ", Output: " + args(2) + ",oracleParams: " + args(3))
}

val input:String = args(1)
val output:String = args(2)
val outputtemp:String = output+"temp"
val outputfinal:String = output+"result"
val para =new test();
para.init(args(0));

case class dpiidcismlog(commandid: String, houseid: String, gathertime: String, srcip: String,
  destip: String, srcport: String, destport: String, domainname: String, proxytype: String,
  proxyip: String, proxyport: String, title: String, content: String, url: String,atname:String)

def search_line_localfile(file_name:String ): List[ dpiidcismlog ]=
{
        var lines=Source.fromFile("/root/bin_ext/zhongying/data.txt").getLines.toList;
        var b=lines.map(_.split("<<<!>>>")).filter(p => p.length >= 15).map(p=>(dpiidcismlog(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14)) ))

        //var c=lines.length;
        //println(file_name+"--->"+c);


        return b;
}


def search_line(file_name:String ,input:org.apache.hadoop.io.BytesWritable): List[ dpiidcismlog]=
{
        var file=new String(input.getBytes);
        var lines=file.split("\n");
        var b=lines.map(_.split("<<<!>>>")).filter(p => p.length >= 15).map(p=>(dpiidcismlog(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14)) )).toList;

        //var c=lines.length;
        //println(file_name+"--->"+c);
        return b;
}

var b=search_line_localfile("sadfa")
var coun=b.filter(p=>p.commandid.toInt==3724).length

filter_str= para.get_filter_str();

val sc = new SparkContext("local", "test")
val rdd2 = sc.sequenceFile[String, org.apache.hadoop.io.BytesWritable]("/root/xinan_2017_04_27.seq")  

var array=rdd2.flatMap( each => search_line(each._1, each._2))
array.filter(p=>p.srcport.toInt==80)
array.filter(p=>p.srcport=="80")
array.filter(p=>p.srcip=="80")


//转换to DF  速度会变很慢. hive 
var value=rdd2.flatMap( each => search_line(each._1, each._2)).toDF();
var value=rdd2.flatMap( each => count_line(each._1, each._2)).toDF();
//value.filter(filter_str);
value.filter("srcport=80");

//导出层 parquet 
//value.filter(strfilter).write.mode("append").parquet(outputtemp)
//value.write.mode("append").parquet(outputtemp)



// 文件分割参考: 
//   xin_an/spark/DPIIDCISMLogQuery.scala

