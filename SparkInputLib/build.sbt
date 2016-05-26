name := "spark_input_lib"

organization := "it.mgaido"

version := "0.0.4"

scalaVersion := "2.10.5"

autoScalaLibrary := false

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.1" % "provided"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.2" % "provided" exclude("javax.servlet", "servlet-api")
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.2" % "provided" exclude("javax.servlet", "servlet-api")
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.2" % "provided" exclude("javax.servlet", "servlet-api")


libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.1.3" % "test"

resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
