name := "spark"

version := "0.1"

scalaVersion := "2.12.15"
autoScalaLibrary := false

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0" //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.2.0" //% "provided"
