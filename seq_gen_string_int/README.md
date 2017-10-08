rm -rf /tmp/uuid/
spark-submit --class   org.apache.spark.greshem.gen_huge_file  --master  local   target/scala-2.11/spark-gen_huge_file_2.11-1.0.0-SNAPSHOT.jar   1  /tmp/uuid/
