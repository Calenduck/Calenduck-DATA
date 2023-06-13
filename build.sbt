import sbt.Keys.libraryDependencies

name := "competitionPipelines"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-core" % "3.3.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.3.2" % "provided",

  "com.databricks" %% "spark-xml" % "0.13.0",
  "org.scala-lang.modules" %% "scala-xml" % "1.2.0",

  "com.facebook.presto" % "presto-jdbc" % "0.258" // 적절한 버전 사용

)

assembly / assemblyJarName := "competitionPipelines.jar"
