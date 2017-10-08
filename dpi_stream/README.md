#监控 /tmp/tmp3/ 的目录有文件的话  就打印出来.
#spark-submit --class   org.apache.spark.greshem.streaming_spark  --master  local   target/scala-2.10/spark-streaming_spark_2.10-1.0.0-SNAPSHOT.jar      /tmp/tmp3/

spark-submit --class   org.apache.spark.greshem.streaming_spark  --master  local   target/scala-2.11/spark-streaming_spark_2.11-1.0.0-SNAPSHOT.jar     /tmp/tmp3/
