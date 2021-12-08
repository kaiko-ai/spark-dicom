package ai.kaiko.spark.dicom

import ai.kaiko.dicom.data.ProxyAttributes
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.dcm4che3.data.Attributes
import org.dcm4che3.io.DicomInputStream
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.time.LocalDate
import java.time.LocalTime

trait WithImplicits {
  implicit val attributesEncoder = Encoders.kryo[Attributes]
}

object TestDicomFileFormat {
  val SOME_DICOM_FILEPATH =
    "src/test/resources/Pancreatic-CT-CBCT-SEG/Pancreas-CT-CB_001/07-06-2012-NA-PANCREAS-59677/201.000000-PANCREAS DI iDose 3-97846/1-001.dcm"
  lazy val SOME_DICOM_FILE = {
    val file = new File(SOME_DICOM_FILEPATH)
    assert(file.exists)
    file
  }
  val SOME_PATIENT_NAME = "Pancreas-CT-CB_001"
  val SOME_STUDY_NAME = "Pancreas-CT-CB_001"
  val SOME_STUDY_DATE = LocalDate.of(2012, 7, 6)
  val SOME_STUDY_TIME = LocalTime.of(11, 18, 23, 360000000)

  val SOME_DICOM_FOLDER_FILEPATH =
    "src/test/resources/Pancreatic-CT-CBCT-SEG/Pancreas-CT-CB_001/07-06-2012-NA-PANCREAS-59677/201.000000-PANCREAS DI iDose 3-97846/*"
}

class TestDicomFileFormat
    extends AnyFunSpec
    with WithLogging
    with WithSpark
    with WithImplicits
    with Matchers
    with BeforeAndAfterAll {

  override protected def afterAll(): Unit = {
    spark.stop
  }

  describe("DicomFileFormat") {
    it("reads to Spark SQL") {
      val df = spark.read
        .format("dicom")
        .load(TestDicomFileFormat.SOME_DICOM_FILEPATH)
        .select(
          "path",
          "content"
        )
    }

    it("reads to Spark SQL Structured Streaming") {
      val df = spark.readStream
        .schema(
          StructType(
            Array(
              StructField("path", StringType, false),
              StructField("content", BinaryType, false)
            )
          )
        )
        .format("dicom")
        .load(
          TestDicomFileFormat.SOME_DICOM_FOLDER_FILEPATH
        )

      val queryName = "testStreamDicom"
      val query = df.writeStream
        .trigger(Trigger.Once)
        .format("memory")
        .queryName(queryName)
        .start

      query.processAllAvailable
      val outDf =
        spark.table(queryName).select("path", "content")
      assert(outDf.count == 79)
    }

    it("decodes to ProxyAttributes") {
      implicit val encoder: Encoder[ProxyAttributes] =
        Encoders.kryo[ProxyAttributes]

      val someAttrs = {
        val dicomInputStream =
          new DicomInputStream(TestDicomFileFormat.SOME_DICOM_FILE)
        dicomInputStream.readDataset
      }

      val df = spark.read
        .format("dicom")
        .load(TestDicomFileFormat.SOME_DICOM_FILEPATH)
        .select("content")
        .as[ProxyAttributes]

      ProxyAttributes.to(df.first) shouldBe someAttrs
    }
  }
}
