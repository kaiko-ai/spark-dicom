package ai.kaiko.spark.dicom

import ai.kaiko.dicom.DicomTagKeyword.tag
import org.apache.hadoop.fs.Path
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.dcm4che3.data
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.funspec.AnyFunSpec

import java.nio.file.Files
import scala.collection.mutable.MutableList
import scala.io.BufferedSource
import scala.io.Source
import ai.kaiko.dicom.TestDicomFile

trait WithSpark {
  var spark = {
    val spark = SparkSession.builder.master("local").getOrCreate
    spark.sparkContext.setLogLevel(Level.ERROR.toString())
    spark
  }
}

object TestDicomFileFormat {
  val SOME_DICOM_FOLDER_FILEPATH =
    "src/test/resources/Pancreatic-CT-CBCT-SEG/Pancreas-CT-CB_001/07-06-2012-NA-PANCREAS-59677/201.000000-PANCREAS DI iDose 3-97846/*"
}

class TestDicomFileFormat
    extends AnyFlatSpec
    with WithSpark
    with BeforeAndAfterAll {

  val logger = LogManager.getLogger("TestDicomFileFormat");
  logger.setLevel(Level.DEBUG)

  override protected def afterAll(): Unit = {
    spark.stop
  }

  "Spark" should "read DICOM files" in {
    val df = spark.read
      .format("dicom")
      .load(TestDicomFile.SOME_DICOM_FILEPATH)
      .select("path", tag(data.Tag.PersonName))
  }

  "Spark" should "stream DICOM files" in {
    val df = spark.readStream
      .schema(DicomFileFormat.DEFAULT_SCHEMA)
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
    val outDf = spark.table(queryName).select("path", tag(data.Tag.PersonName))
    assert(outDf.count == 79)
  }

  "Spark" should "write a DICOM file" in {
    val df = spark.read
      .format("dicom")
      .load(TestDicomFile.SOME_DICOM_FILEPATH)
      .select("path", tag(data.Tag.PersonName))

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
