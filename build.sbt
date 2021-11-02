name := "spark-dicom"

version := "0.1"

scalaVersion := "2.12.15"

idePackagePrefix := Some("ai.kaiko")

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.2.0" % "provided"