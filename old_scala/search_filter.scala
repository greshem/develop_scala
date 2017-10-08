#/hadoop/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class com.zhongying.dpi.dpiidcismlog.query.DPIIDCISMLogQuery --master local --executor-memory 2g --num-executors 3 --driver-class-path /hadoop/spark-1.6.0-bin-hadoop2.6/lib/ojdbc6-11.2.0.3.jar /slview/nms/bin/java/DPIIDCISMLogQuery.jar sys=city,cmdtype=,commandid=11,houseId=237,starttime=1459930200,endtime=1459930400,srcip=,srcport=,destip=,destport=,domainname= /hadoop/DPIIDCISMLOG/ /hadoop/mapreduce/20160422/254@@@@@@@@@1459930200@1459930400@013@/ jdbc:oracle:thin:@172.16.34.87:1521:dbnms,dpi_iteview_test,oracle

#  最后查询 rdd filter 语法变成:  String = houseid = '237' and commandid = '11' and gathertime >= 1459930200 and gathertime <= 1459930400
#



  #val input:String = args(1)
  val input:String = "/hadoop/DPIIDCISMLOG/"
  #val output:String = args(2)
  val output:String = "/hadoop/mapreduce/20160422/254@@@@@@@@@1459930200@1459930400@013@/"

  val outputtemp:String = output+"temp"
  val outputfinal:String = output+"result"

  #val params: Array[String] = args(0).split(",")
  val params: Array[String] = "sys=city,cmdtype=,commandid=11,houseId=237,starttime=1459930200,endtime=1459930400,srcip=,srcport=,destip=,destport=,domainname=".split(",")

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


        #生成查询 过滤字符串
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

