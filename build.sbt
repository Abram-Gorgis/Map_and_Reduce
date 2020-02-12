
name := "Homework2"

version := "0.1"

scalaVersion := "2.13.1"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.2.1",
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-core
 "org.apache.hadoop" % "hadoop-core" % "1.2.1",
// https://mvnrepository.com/artifact/org.apache.commons/commons-lang3
 "org.apache.commons" % "commons-lang3" % "3.4",
// https://mvnrepository.com/artifact/org.slf4j/slf4j-api
 "org.slf4j" % "slf4j-api" % "1.7.28",
// https://mvnrepository.com/artifact/ch.qos.logback/logback-core
"ch.qos.logback" % "logback-core" % "1.2.3",
// https://mvnrepository.com/artifact/org.junit.jupiter/junit-jupiter-api
 "junit" % "junit" % "4.12",
// https://mvnrepository.com/artifact/org.scalatest/scalatest
 "org.scalatest" %% "scalatest" % "3.2.0-M1" % Test,
  "org.scala-lang.modules" %% "scala-xml" % "1.2.0",
 "org.apache.commons" % "commons-text" % "1.4",
  "com.typesafe" % "config" % "1.3.3"
)

fork:= true

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
