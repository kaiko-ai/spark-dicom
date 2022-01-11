package ai.kaiko.spark.dicom

import ai.kaiko.spark.dicom.v2.DicomDataSource
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger
import org.dcm4che3.data.Keyword.{valueOf => keywordOf}
import org.dcm4che3.data._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.CancelAfterFailure
import org.scalatest.funspec.AnyFunSpec

import java.io.File
import java.time.LocalDate
import java.time.LocalTime
import java.time.format.DateTimeFormatter

trait WithSpark {
  var spark = {
    val spark = SparkSession.builder.master("local").getOrCreate
    spark.sparkContext.setLogLevel(Level.ERROR.toString())
    spark
  }
}

object TestDicomDataSource {
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

class TestDicomDataSource
    extends AnyFunSpec
    with WithSpark
    with BeforeAndAfterAll
    with CancelAfterFailure {
  import TestDicomDataSource._

  val logger = {
    val logger = LogManager.getLogger(getClass.getName);
    logger.setLevel(Level.DEBUG)
    logger
  }

  override protected def afterAll(): Unit = {
    spark.stop
  }

  describe("Spark") {
    it("reads DICOM files") {
      val df = spark.read
        .format("dicomFile")
        .load(SOME_DICOM_FILEPATH)
        .select(
          col("path"),
          col(keywordOf(Tag.PatientName)),
          col(keywordOf(Tag.StudyDate)),
          col(keywordOf(Tag.StudyTime))
        )

      val row = df.first

      assert(
        row.getAs[Row](keywordOf(Tag.PatientName)).getAs[String](0)
          === SOME_STUDY_NAME
      )
      assert(
        row.getAs[String](keywordOf(Tag.StudyDate))
          === SOME_STUDY_DATE.format(
            DateTimeFormatter.ISO_LOCAL_DATE
          )
      )
      assert(
        row.getAs[String](keywordOf(Tag.StudyTime))
          === SOME_STUDY_TIME.format(
            DateTimeFormatter.ISO_LOCAL_TIME
          )
      )
    }

    it("reads all attributes of DICOM files") {
      val df = spark.read
        .format("dicomFile")
        .load(SOME_DICOM_FILEPATH)
        .select("*")

      val row = df.first
    }

    describe("reads metadata") {
      it("path") {
        val df = spark.read
          .format("dicomFile")
          .load(SOME_DICOM_FILEPATH)
          .select("path")

        val row = df.first

        // abs vr rel path
        assert(row.getAs[String]("path").endsWith(SOME_DICOM_FILEPATH))
      }
    }

    it(
      "does not allow reading PixelData when not specified explicitly in config"
    ) {
      val thrown = intercept[org.apache.spark.sql.AnalysisException](
        spark.read
          .format("dicomFile")
          .load(SOME_DICOM_FILEPATH)
          .select(
            col("path"),
            col(keywordOf(Tag.PixelData))
          )
          .first
          .getAs[Array[Byte]](keywordOf(Tag.PixelData))
      )
      assert(thrown.message.startsWith("cannot resolve 'PixelData'"))
    }

    it("allows reading PixelData when specified explicitly in config") {
      assert(
        DicomDataSource
          .schema(withPixelData = true)
          .fields
          .find(_.name == keywordOf(Tag.PixelData))
          .isDefined
      )
      assert(
        spark.read
          .format("dicomFile")
          .option(DicomDataSource.OPTION_WITHPIXELDATA, true)
          .load(SOME_DICOM_FILEPATH)
          .select(
            col("path"),
            col(keywordOf(Tag.PixelData))
          )
          .first
          .getAs[Array[Byte]](keywordOf(Tag.PixelData))
          .size > 0
      )
    }

    it("reads a stream of DICOM files") {
      val df = spark.readStream
        .schema(DicomDataSource.schema(false))
        .format("dicomFile")
        .load(SOME_DICOM_FOLDER_FILEPATH)
        .select(
          col("path"),
          col(f"${keywordOf(Tag.PatientName)}.Alphabetic")
            .as(keywordOf(Tag.PatientName)),
          col(keywordOf(Tag.StudyDate)),
          col(keywordOf(Tag.StudyTime))
        )

      val queryName = "testStreamDicom"
      val query = df.writeStream
        .trigger(Trigger.Once)
        .format("memory")
        .queryName(queryName)
        .start

      query.processAllAvailable
      val outDf =
        spark.table(queryName).select("path", keywordOf(Tag.PatientName))
      assert(outDf.count == 79)
    }
  }
}
