package ai.kaiko.spark.dicom

import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.file.Files
import scala.collection.mutable.MutableList
import scala.io.BufferedSource
import scala.io.Source

import funspec._

trait WithSpark {
  var spark = {
    val spark = SparkSession.builder.master("local").getOrCreate
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }
}

class TestDicomFileFormat
    extends AnyFlatSpec
    with WithSpark
    with BeforeAndAfterAll {

  override protected def afterAll(): Unit = {
    spark.stop
  }

  "Spark" should "read DICOM files" in {
    val df = spark.read
      .format("dicom")
      .load(
        "src/test/resources/Pancreatic-CT-CBCT-SEG/Pancreas-CT-CB_001/07-06-2012-NA-PANCREAS-59677/1.000000-BSCBLLLRSDCB-27748/1-1.dcm"
      )
      .select("path", "length", "content")
    df.show
  }

  "Spark" should "stream DICOM files" in {
    val df = spark.readStream
      .schema(DicomFileFormat.SCHEMA)
      .format("dicom")
      .load(
        "src/test/resources/Pancreatic-CT-CBCT-SEG/Pancreas-CT-CB_001/*/*/*"
      )

    val queryName = "testStreamDicom"
    val query = df.writeStream
      .trigger(Trigger.Once)
      .format("memory")
      .queryName(queryName)
      .start

    query.processAllAvailable
    spark.table(queryName).show
  }

  "Spark" should "write a DICOM file" in {
    val df = spark.read
      .format("dicom")
      .load(
        "src/test/resources/Pancreatic-CT-CBCT-SEG/Pancreas-CT-CB_001/07-06-2012-NA-PANCREAS-59677/1.000000-BSCBLLLRSDCB-27748/1-1.dcm"
      )
      .select("path", "length", "content")

    val tmpDir = Files.createTempDirectory("some-dicom-files").toFile
    val outFile = tmpDir.toPath().resolve("1-1.dcm")

    df.repartition(1).write.format("dicom").save(outFile.toString)
  }
}
