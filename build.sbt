name := "spark-dicom"

inThisBuild(
  List(
    scalaVersion := "2.12.15",
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision, // only required for Scala 2.x
    scalacOptions += "-Ywarn-unused-import", // Scala 2.x only, required by `RemoveUnused`
    scalafixDependencies += "com.github.liancheng" %% "organize-imports" % "0.6.0",
    versionScheme := Some("early-semver")
  )
)
idePackagePrefix := Some("ai.kaiko")
Global / excludeLintKeys += idePackagePrefix

resolvers += "dcm4che Repository" at "https://www.dcm4che.org/maven2"

libraryDependencies += "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.17.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.2.0" % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % "test"
libraryDependencies += "org.dcm4che" % "dcm4che-core" % "5.24.2"
libraryDependencies += "org.dcm4che" % "dcm4che-json" % "5.24.2"
libraryDependencies += "org.dcm4che" % "dcm4che-imageio" % "5.24.2"
libraryDependencies += "javax.json" % "javax.json-api" % "1.1.4"

// needed to allow separate test classes to run in their own SparkSession
Test / parallelExecution := false

// assembly / mainClass := Some("ai.kaiko.dicom.app.Main")
ThisBuild / organization := "ai.kaiko"
ThisBuild / organizationName := "Kaiko"
ThisBuild / organizationHomepage := Some(url("https://kaiko.ai"))
ThisBuild / sonatypeCredentialHost := "s01.oss.sonatype.org"
ThisBuild / crossPaths := false
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/kaiko-ai/spark-dicom"),
    "scm:git@github.com:kaiko-ai/spark-dicom.git"
  )
)

ThisBuild / developers := List(
  Developer(
    id = "marijncv",
    name = "Marijn Valk",
    email = "marijncv@hotmail",
    url = url("https://github.com/marijncv")
  ),
  Developer(
    id = "GuillaumeDesforges",
    name = "Guillaume Desforges",
    email = "guillaume.desforges.pro@gmail.com",
    url = url("https://github.com/GuillaumeDesforges")
  ),
  Developer(
    id = "robopoc",
    name = "Robert Berke",
    email = "berke.robert@gmail.com",
    url = url("https://github.com/robopoc")
  )
)

ThisBuild / description := "Spark DICOM connector in Scala"
ThisBuild / licenses := List(
  "Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt")
)
ThisBuild / homepage := Some(url("https://github.com/kaiko-ai/spark-dicom"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := sonatypePublishToBundle.value

Global / excludeLintKeys += pomIncludeRepository
