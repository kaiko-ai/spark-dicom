package ai.kaiko.spark.dicom.v1

import ai.kaiko.spark.dicom.DicomFileReader
import ai.kaiko.spark.dicom.DicomStandardSpark
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

object DicomFileFormat {
  val FIELD_NAME_PATH = "path"
  val SCHEMA = StructType(
    StructField(FIELD_NAME_PATH, StringType, false) +: DicomStandardSpark.fields
  )
}

class DicomFileFormat
    extends FileFormat
    with DataSourceRegister
    with Serializable {

  import DicomFileFormat._

  override def shortName(): String = "dicomFile"

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]
  ): Option[StructType] = Some(SCHEMA)

  override protected def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration
  ): PartitionedFile => Iterator[InternalRow] = {
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(
        new SerializableConfiguration(hadoopConf)
      )

    DicomFileReader.readDicomFile(
      dataSchema,
      partitionSchema,
      requiredSchema,
      filters,
      broadcastedHadoopConf,
      _
    )
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType
  ): OutputWriterFactory =
    throw new NotImplementedError("Writing to files is not supported")

}
