package ai.kaiko.spark.dicom

import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.dcm4che3.data.Keyword.{valueOf => keywordOf}
import org.dcm4che3.data.Tag
import org.scalatest.BeforeAndAfterAll
import org.scalatest.CancelAfterFailure
import org.scalatest.flatspec.AnyFlatSpec

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
    extends AnyFlatSpec
    with WithSpark
    with BeforeAndAfterAll
    with CancelAfterFailure {

  val logger = LogManager.getLogger("TestDicomFileFormat");
  logger.setLevel(Level.DEBUG)

  override protected def afterAll(): Unit = {
    spark.stop
  }

  "Spark" should "read DICOM files" in {
    val df = spark.read
      .format("dicomFile")
      .load(TestDicomFileFormat.SOME_DICOM_FILEPATH)
      .select(
        col("path"),
        col(f"${keywordOf(Tag.PatientName)}.Alphabetic")
          .as(keywordOf(Tag.PatientName)),
        col(keywordOf(Tag.StudyDate)),
        col(keywordOf(Tag.StudyTime))
      )

    val row = df.first
    assert(
      row.getAs[String](
        keywordOf(Tag.PatientName)
      ) === TestDicomFileFormat.SOME_STUDY_NAME
    )
    assert(
      row.getAs[LocalDate](
        keywordOf(Tag.StudyDate)
      ) === TestDicomFileFormat.SOME_STUDY_DATE.format(
        DateTimeFormatter.ISO_LOCAL_DATE
      )
    )
    assert(
      row.getAs[LocalTime](
        keywordOf(Tag.StudyTime)
      ) === TestDicomFileFormat.SOME_STUDY_TIME.format(
        DateTimeFormatter.ISO_LOCAL_TIME
      )
    )
  }

  "Spark" should "stream DICOM files" in {
    val df = spark.readStream
      .schema(
        StructType(
          StructField("path", StringType, false) +: DicomStandardSpark.fields
        )
      )
      .format("dicomFile")
      .load(
        TestDicomFileFormat.SOME_DICOM_FOLDER_FILEPATH
      )
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
