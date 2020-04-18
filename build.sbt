name := "DI_Framework"

version := "0.1"

scalaVersion := "2.11.12"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.11.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.apache.kafka" %% "kafka" % "1.0.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.0" % "provided"

