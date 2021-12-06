package ai.kaiko.spark.dicom

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File
import java.nio.file.Files
import java.time.LocalDate
import java.time.LocalTime
import org.apache.spark.sql.Encoders
import org.dcm4che3.data.Attributes

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
    extends AnyFlatSpec
    with WithLogging
    with WithSpark
    with WithImplicits
    with BeforeAndAfterAll {

  override protected def afterAll(): Unit = {
    spark.stop
  }

  "Spark" should "read DICOM files" in {
    val df = spark.read
      .format("dicom")
      .load(TestDicomFileFormat.SOME_DICOM_FILEPATH)
      .select(
        "path",
        "content"
      )

    df.show
  }

  "Spark" should "stream DICOM files" in {
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

  "Spark" should "write a DICOM file" in {
    val df = spark.read
      .format("dicom")
      .load(TestDicomFileFormat.SOME_DICOM_FILEPATH)
      .select("path", "content")

    val tmpDir = Files.createTempDirectory("some-dicom-files")
    tmpDir.toFile.delete // need to delete since Spark handles creation
    val tmpPath = new Path(tmpDir.toUri)
    val outPath = new Path(tmpDir.resolve("1-1.dcm").toUri)

    // write to single
    df.repartition(1)
      .write
      .format("dicom")
      .save(tmpDir.toAbsolutePath.toString)
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = tmpPath.getFileSystem(conf)
    val oneFile = fs
      .listStatus(tmpPath)
      .map(x => x.getPath.toString)
      .find(x => x.endsWith(".dcm"))
    val srcFile = new Path(oneFile.get)
    fs.rename(srcFile, outPath)

    logger.info("Write out to : " + outPath.toString)
  }
}
