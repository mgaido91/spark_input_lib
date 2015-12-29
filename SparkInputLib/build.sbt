name := "spark_input_lib"

organization := "it.mgaido"

version := "0.0.3"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.2.0-cdh5.3.2" % "provided"
libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.1.3" % "test"

resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
