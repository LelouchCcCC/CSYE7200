ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.7"

libraryDependencies ++= Seq( "org.apache.spark" %% "spark-sql" % "3.5.0",
"org.apache.spark" %% "spark-mllib" % "3.5.0")

lazy val root = (project in file("."))
  .settings(
    name := "Spark2"
  )
