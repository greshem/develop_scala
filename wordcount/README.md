#cp /etc/passwd   /tmp/passwd
#sed -i  's/:/ /g' /tmp/passwd

spark-submit --class   org.apache.spark.greshem.wordcount  --master  local   target/scala-2.11/spark-wordcount_2.11-1.0.0-SNAPSHOT.jar     /tmp/passwd /tmp/output/






