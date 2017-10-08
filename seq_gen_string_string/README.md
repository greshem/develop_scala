#rm -rf /tmp/uuid/
#spark-submit --class   org.apache.spark.greshem.gen_huge_file  --master  local   target/scala-2.11/spark-gen_huge_file_2.11-1.0.0-SNAPSHOT.jar   1  /tmp/uuid/
spark-submit --class   org.apache.spark.greshem.gen_huge_file --executor-memory 2g --num-executors 4    --master  local   target/scala-2.10/spark-gen_huge_file_2.10-1.0.0-SNAPSHOT.jar    8  /tmp/uuid/
