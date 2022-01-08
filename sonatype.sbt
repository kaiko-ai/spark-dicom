sonatypeProfileName := "ai.kaiko"

publishMavenStyle := true

licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

import xerial.sbt.Sonatype._
sonatypeProjectHosting := Some(GitHubHosting("kaiko-ai", "spark-dicom", "info@kaiko.ai"))

homepage := Some(url("https://github.com/kaiko-ai/spark-dicom"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/kaiko-ai/spark-dicom"),
    "scm:git@github.com:kaiko-ai/spark-dicom.git"
  )
)
developers := List(
  Developer(id="marijncv", name="Marijn Valk", email="marijn@kaiko.ai", url=url("https://github.com/marijncv"))
)