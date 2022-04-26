ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "Ty-Data-Bootcamp-April-2022-Spark",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"
  )
