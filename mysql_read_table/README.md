#spark-submit --class  me.soulmachine.spark.LoadJDBCTest  --master  local   target/scala-2.11/spark-mysql_2.11-1.0.0-SNAPSHOT.jar 

#jar2=/root/.m2/repository/mysql/mysql-connector-java/5.1.38/mysql-connector-java-5.1.38.jar 
jar=/root/.m2/repository/mysql/mysql-connector-java/5.1.17/mysql-connector-java-5.1.17.jar

spark-submit --class   org.apache.spark.examples.sql.LoadJDBCTest   --driver-class-path $jar \
--master  local   target/scala-2.10/spark-mysql_2.10-1.0.0-SNAPSHOT.jar    jdbc:mysql://localhost:3306/mysql user    root password



