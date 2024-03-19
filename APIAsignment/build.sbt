ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

libraryDependencies += "com.softwaremill.sttp.client4" %% "core" % "4.0.0-M11"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.9.2"

lazy val root = (project in file("."))
  .settings(
    name := "API"
  )
