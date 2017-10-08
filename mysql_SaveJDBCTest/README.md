jar=/root/.m2/repository/mysql/mysql-connector-java/5.1.17/mysql-connector-java-5.1.17.jar

spark-submit --class   org.apache.spark.greshem.SaveJDBCTest   --driver-class-path $jar   --master  local   target/scala-2.10/spark-savejdbctest_2.10-1.0.0-SNAPSHOT.jar      jdbc:mysql://localhost:3306/mysql user    root password

