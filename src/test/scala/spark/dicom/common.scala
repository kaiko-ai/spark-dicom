package ai.kaiko.spark.dicom

import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

trait WithLogging {
  val logger = LogManager.getLogger("spark-dicom-test");
  logger.setLevel(Level.DEBUG)
}

trait WithSpark {
  var spark = {
    val spark = SparkSession.builder.master("local").getOrCreate
    spark.sparkContext.setLogLevel(Level.ERROR.toString())
    spark
  }
}
