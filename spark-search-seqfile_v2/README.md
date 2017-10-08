#参考  ../spark-example-project/
#==========================================================================
#https://github.com/soulmachine/spark-example-project


#spark-submit --class me.soulmachine.spark.WordCount --master yarn://ip-or-host:7077 ./spark-example-project-1.0.jar wordcount-test/input wordcount-test/outputs

spark-submit --class me.soulmachine.spark.WordCount --master local target/scala-2.10/spark-example-project_2.10-1.0.0-SNAPSHOT.jar
