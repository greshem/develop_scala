#spark-submit --class   org.apache.spark.greshem.DataFrameTest  --master  local   target/scala-2.11/spark-dataframetest_2.11-1.0.0-SNAPSHOT.jar   \
#/root/bin_ext/spark/SparkDemo/src/main/resources/weather.txt  /tmp/bbbb/


spark-submit --class   org.apache.spark.greshem.DataFrameTest  --master  local   target/scala-2.10/spark-dataframetest_2.10-1.0.0-SNAPSHOT.jar   \
/root/bin_ext/spark/SparkDemo/src/main/resources/weather.txt  /tmp/bbbb/






### 示例8. SQL 使用示例

#### 1 DataFrame 的基本操作: [DataFrameTest](/src/main/scala/org/apache/spark/examples/sql/DataFrameTest.scala)

#该示例主要展示 DataFrame 的创建、基本操作、以及 Schema inference 相关的内容，数据以 json 为例。

#另外，数据在 resources 目录下有示例，为 weather.txt。
#
#代码提交方式如下:

```

#[qifeng.dai@bgsbtsp0006-dqf sparkbook]$ spark-submit --class org.apache.spark.examples.sql.DataFrameTest \
#                                        --master yarn \
#                                        --deploy-mode cluster \
#                                        --driver-cores 1 \
#                                        --driver-memory 1024M \
#                                        --num-executors 4 \
#                                        --executor-cores 2 \
#                                        --executor-memory 4096M \
#                                        spark-examples-1.0-SNAPSHOT-hadoop2.6.0.jar /user/qifeng.dai/input/weather.txt /user/qifeng.dai/output/weather
#```

