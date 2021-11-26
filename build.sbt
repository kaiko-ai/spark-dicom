name := "spark-dicom"

version := "0.1"

inThisBuild(
  List(
    scalaVersion := "2.12.15",
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision, // only required for Scala 2.x
    scalacOptions += "-Ywarn-unused-import", // Scala 2.x only, required by `RemoveUnused`
    scalafixDependencies += "com.github.liancheng" %% "organize-imports" % "0.6.0"
  )
)
idePackagePrefix := Some("ai.kaiko")
Global / excludeLintKeys += idePackagePrefix

resolvers += "dcm4che Repository" at "https://www.dcm4che.org/maven2"

libraryDependencies += "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.14.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.2.0" % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % "test"
libraryDependencies += "org.dcm4che" % "dcm4che-core" % "5.24.2"
libraryDependencies += "org.dcm4che" % "dcm4che-imageio" % "5.24.2"

// assembly / mainClass := Some("ai.kaiko.dicom.app.Main")
