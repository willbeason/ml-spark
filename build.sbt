name := "ml-spark"

version := "0.1"

// Spark requires Scala 2.11
scalaVersion := "2.11.12"

libraryDependencies += "org.apache.commons" % "commons-csv" % "1.5"


// Don't use the latest versions. Upgrading will cause horrible failures.
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"

val hadoopVersion = "2.8.3"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoopVersion

libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.297"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.0"
