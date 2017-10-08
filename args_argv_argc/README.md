set -x

dir=$1
spark-submit --class   org.apache.spark.greshem.gen_huge_file  --master  local   target/scala-2.10/argv_argc_args_2.10-1.0.0-SNAPSHOT.jar $dir
