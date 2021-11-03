package ai.kaiko.spark.dicom

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter

class DicomFileFormat extends FileFormat with DataSourceRegister {

  import DicomFileFormat._

  override def shortName(): String = DICOM

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]
  ): Option[StructType] = Some(schema)

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType
  ): OutputWriterFactory = ???

  override protected def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration
  ): PartitionedFile => Iterator[InternalRow] = ???

}

object DicomFileFormat {

  private[DicomFileFormat] val DICOM   = "dicom"
  private[DicomFileFormat] val LENGTH  = "length"
  private[DicomFileFormat] val CONTENT = "content"

  val schema = StructType(
    StructField(LENGTH, LongType, false) ::
      StructField(CONTENT, BinaryType, true) :: Nil
  )

}
