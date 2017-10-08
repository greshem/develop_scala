name := "spark-gen_huge_file"

version := "1.0.0-SNAPSHOT"

licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

scalaVersion := "2.10.5"
val sparkVersion = "1.6.3"

fork := true

fork in console := true

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

javaOptions ++= Seq("-Xmx2G")

scalacOptions ++= Seq("-deprecation", "-unchecked")

resolvers ++= Seq(
  Resolver.defaultLocal,
  Resolver.mavenLocal,
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/"
  )


val sparkDependencyScope = "provided"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-sql" % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % sparkDependencyScope
)

// If you want to use yarn-client for spark cluster mode, override the environment variable
// SPARK_MODE=yarn-client <cmd>
val sparkMode = sys.env.getOrElse("SPARK_MODE", "local[2]")


initialCommands in console :=
  s"""
    |import org.apache.spark.SparkConf
    |import org.apache.spark.SparkContext
    |import org.apache.spark.SparkContext._
    |
    |@transient val sc = new SparkContext(
    |  new SparkConf()
    |    .setMaster("$sparkMode")
    |    .setAppName("Console test"))
    |implicit def sparkContext = sc
    |import sc._
    |
    |@transient val sqlc = new org.apache.spark.sql.SQLContext(sc)
    |implicit def sqlContext = sqlc
    |import sqlc._
    |
    |def time[T](f: => T): T = {
    |  import System.{currentTimeMillis => now}
    |  val start = now
    |  try { f } finally { println("Elapsed: " + (now - start)/1000.0 + " s") }
    |}
    |
    |""".stripMargin

cleanupCommands in console :=
  s"""
     |sc.stop()
   """.stripMargin


/// scaladoc
scalacOptions in (Compile,doc) ++= Seq("-groups", "-implicits",
  // NOTE: remember to change the JVM path that works on your system.
  // Current setting should work for JDK7 on OSX and Linux (Ubuntu)
  "-doc-external-doc:/Library/Java/JavaVirtualMachines/jdk1.7.0_60.jdk/Contents/Home/jre/lib/rt.jar#http://docs.oracle.com/javase/7/docs/api",
  "-doc-external-doc:/usr/lib/jvm/java-7-openjdk-amd64/jre/lib/rt.jar#http://docs.oracle.com/javase/7/docs/api"
  )

autoAPIMappings := true

