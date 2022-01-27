package ai.kaiko.spark

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level

import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suites

import ai.kaiko.spark.dicom.deidentifier.TestDicomDeidentifier
import ai.kaiko.spark.dicom.TestDicomDataSource

trait WithSpark {
  var spark = {
    val spark = SparkSession.builder.master("local").getOrCreate
    spark.sparkContext.setLogLevel(Level.ERROR.toString())
    spark
  }
}

// Here we make sure to include all tests that require a sparkSession.
// We close the spark session after all tests in the suite have run.
class TestSparkSuite extends Suites(
  new TestDicomDeidentifier, 
  new TestDicomDataSource
) 
  with WithSpark 
  with BeforeAndAfterAll {

  override protected def afterAll(): Unit = {
    spark.stop
  }   
}
