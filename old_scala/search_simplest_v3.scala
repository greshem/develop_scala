
case class dpiidcismlog(commandid: String, houseid: String, gathertime: String, srcip: String,
  destip: String, srcport: String, destport: String, domainname: String, proxytype: String,
  proxyip: String, proxyport: String, title: String, content: String, url: String,atname:String)

def search_line(file_name:String ,input:org.apache.hadoop.io.BytesWritable): List[ dpiidcismlog]=
{
        var file=new String(input.getBytes);
        var lines=file.split("\n");
        var b=lines.map(_.split("<<<!>>>")).filter(p => p.length >= 15).map(p=>(dpiidcismlog(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14)) )).toList;

        //var c=lines.length;
        //println(file_name+"--->"+c);
        return b;
}



//val rdd2 = sc.sequenceFile[String, org.apache.hadoop.io.BytesWritable]("/root/xinan_2017_04_27.seq")  
val rdd2 = sc.sequenceFile[String, org.apache.hadoop.io.BytesWritable]("file:///root/xinan_2017_04_27.seq")  

var array=rdd2.flatMap( each => search_line(each._1, each._2))
//array.filter(p=>p.srcport.toInt==80)
//array.filter(p=>p.srcport=="80")
array.filter(p=>p.srcip=="80").count


