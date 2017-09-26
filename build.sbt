name := "QuantPM"

version := "0.1"

scalaVersion := "2.12.3"

libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.7-dmr"
libraryDependencies +="org.mongodb.scala" %% "mongo-scala-driver" % "2.1.0"

// https://mvnrepository.com/artifact/com.typesafe/config
libraryDependencies += "com.typesafe" % "config" % "1.3.1"