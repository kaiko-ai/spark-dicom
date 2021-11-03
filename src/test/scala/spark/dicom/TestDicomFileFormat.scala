package ai.kaiko.spark.dicom

import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable.MutableList
import scala.io.BufferedSource
import scala.io.Source

class TestDicomFileFormat extends AnyFlatSpec {
  "Spark" should "read DICOM files" in {
    val spark = SparkSession.builder.master("local").getOrCreate
    try {
      val df = spark.read
        .format("dicom")
        .load(
          "src/test/resources/Pancreatic-CT-CBCT-SEG/Pancreas-CT-CB_001/07-06-2012-NA-PANCREAS-59677/1.000000-BSCBLLLRSDCB-27748/1-1.dcm"
        )
        .select("path", "length", "content")
      df.show
    } finally {
      spark.stop
    }
  }

  "Spark" should "stream DICOM files" in {
    val spark = SparkSession.builder.master("local").getOrCreate
    try {
      val df = spark.readStream
        .schema(DicomFileFormat.SCHEMA)
        .format("dicom")
        .load(
          "src/test/resources/Pancreatic-CT-CBCT-SEG/Pancreas-CT-CB_001/07-06-2012-NA-PANCREAS-59677/1.000000-BSCBLLLRSDCB-27748/"
        )

      val queryName = "testStreamDicom"
      val query = df.writeStream
        .trigger(Trigger.Once)
        .format("memory")
        .queryName(queryName)
        .start

      query.processAllAvailable
      spark.table(queryName).show

    } finally {
      spark.stop
    }
  }
}
