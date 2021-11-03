package spark.dicom

import ai.kaiko.spark.dicom.DicomFileFormat
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

import scala.io.BufferedSource
import scala.io.Source

class TestDicomFileFormat extends AnyFlatSpec {
  "Spark" should "read DICOM files" in {
    val spark = SparkSession.builder.master("local").getOrCreate
    try {
      val df = spark.read
        // TODO use "dicom"
        .format("dicom")
        .load(
          "src/test/resources/Pancreatic-CT-CBCT-SEG/Pancreas-CT-CB_001/07-06-2012-NA-PANCREAS-59677/1.000000-BSCBLLLRSDCB-27748/1-1.dcm"
        )
      df.show
      // TODO have a proper assertion here
    } finally {
      spark.stop
    }
  }
}
