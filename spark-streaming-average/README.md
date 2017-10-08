spark-streaming-average
============================
#https://github.com/rshepherd/spark-streaming-average

#An example of streaming average calculation with spark streaming. Build and deploy with below (or something like it).

sbt package && spark-submit  --class "StreamingAverage"  --master local[2] target/scala-2.10/streaming-average_2.10-1.6.3.jar
