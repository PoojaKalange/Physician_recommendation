name := """Spark_Play_sbt"""

herokuAppName in Compile := "recommendationspark"

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)
lazy val spark = "2.1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  //jdbc,
  cache,
  //ws,
  "org.apache.hadoop"  % "hadoop-client" % "2.7.0",
  "org.apache.spark" %% "spark-core" % spark,
  "org.apache.spark" %% "spark-sql" % spark,
  "org.apache.spark" %% "spark-mllib" % spark,
  "org.postgresql" % "postgresql" % "9.4-1200-jdbc41",
  "org.scalatestplus.play" %% "scalatestplus-play" % "1.5.1" % Test
  
)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.5"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"

 




fork in run := true
