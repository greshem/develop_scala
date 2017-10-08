
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



params.foreach(println);
