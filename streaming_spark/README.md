#监控 /tmp/tmp3/ 的目录有文件的话  就打印出来.
sbt package
jar=/root/.m2/repository/com/google/guava/guava/18.0/guava-18.0.jar
spark-submit   --driver-class-path $jar   --class   org.apache.spark.greshem.streaming_spark  --master  local[3]   target/scala-2.10/spark-streaming_spark_2.10-1.0.0-SNAPSHOT.jar     /tmp/tmp3/
