sbt package
#time spark-submit --class   org.apache.spark.greshem.seq_read  --master  local[8]   target/scala-2.10/spark-seq_read_iobytes_2.10-1.0.0-SNAPSHOT.jar /root/xinan_2017_04_27.seq \
time spark-submit --class   org.apache.spark.greshem.seq_read  --master  local[8]   target/scala-2.10/spark-seq_read_iobytes_2.10-1.0.0-SNAPSHOT.jar /root/xinan_2017_05_06_5510.seq \
-D mapreduce.input.fileinputformat.split.minsize=134217728 \
-D mapreduce.input.fileinputformat.split.maxsize=512000000 

#-D mapred.linerecordreader.maxlength=32768　 \


#统计文件数 
#//println(data.count()); 时间上的差异比较明显  25-40 秒.
#25-29-32-35.808s -40 秒之间  local[8] 的情况下


#统计行数  36秒-34秒 比较固定.
#   var b=data.map(each=>each._2.split("\n").length)
#    println(b.reduce((x,y)=> x+y))

