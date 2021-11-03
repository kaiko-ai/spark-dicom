name := "spark-dicom"

version := "0.1"

scalaVersion := "2.12.15"

idePackagePrefix := Some("ai.kaiko")
Global / excludeLintKeys += idePackagePrefix

resolvers += "dcm4che Repository" at "https://www.dcm4che.org/maven2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql"  % "3.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.2.0" % "provided"
libraryDependencies += "org.scalactic" %% "scalactic"    % "3.2.10"
libraryDependencies += "org.scalatest" %% "scalatest"    % "3.2.10" % "test"
libraryDependencies += "org.dcm4che"    % "dcm4che-core" % "5.24.2"

assembly / mainClass := Some("ai.kaiko.dicom.app.Main")
