sonatypeProfileName := "ai.kaiko"

publishMavenStyle := true

import xerial.sbt.Sonatype._
sonatypeProjectHosting := Some(
  GitHubHosting("kaiko-ai", "spark-dicom", "info@kaiko.ai")
)
