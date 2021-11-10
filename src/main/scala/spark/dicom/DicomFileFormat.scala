package ai.kaiko.spark.dicom

import ai.kaiko.spark.dicom.v2.DicomWrite
import com.google.common.io.ByteStreams
import com.google.common.io.Closeables
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.CodecStreams
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.SQLConf.SOURCES_BINARY_FILE_MAX_LENGTH
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.dcm4che3.data.UID
import org.dcm4che3.io.DicomInputStream
import org.dcm4che3.io.DicomOutputStream

import java.net.URI

object DicomFileFormat {
  val PATH = "path"
  val LENGTH = "length"
  val CONTENT = "content"
  val SCHEMA = StructType(
    StructField(PATH, StringType, false) ::
      StructField(LENGTH, LongType, false) ::
      StructField(CONTENT, BinaryType, true) :: Nil
  )
}

class DicomFileFormat
    extends FileFormat
    with DataSourceRegister
    with Serializable {

  import DicomFileFormat._;

  override def shortName(): String = "dicom"

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
    val maxLength =
      sparkSession.conf.get(SOURCES_BINARY_FILE_MAX_LENGTH.key).toInt

    (file: PartitionedFile) => {
      val path = new Path(new URI(file.filePath))
      val fs = path.getFileSystem(broadcastedHadoopConf.value.value)
      val status = fs.getFileStatus(path)
      // TODO filters

      if (status.getLen > maxLength) {
        throw QueryExecutionErrors.fileLengthExceedsMaxLengthError(
          status,
          maxLength
        )
      }

      val writer = new UnsafeRowWriter(requiredSchema.length)
      writer.resetRowWriter()
      requiredSchema.fieldNames.zipWithIndex.foreach {
        case (PATH, i) =>
          writer.write(i, UTF8String.fromString(status.getPath.toString))
        case (LENGTH, i) => writer.write(i, status.getLen)
        case (CONTENT, i) =>
          if (status.getLen > maxLength) {
            throw QueryExecutionErrors.fileLengthExceedsMaxLengthError(
              status,
              maxLength
            )
          }
          val fileStream = fs.open(status.getPath);
          val dicomFileStream = new DicomInputStream(fileStream);
          try {
            writer.write(i, ByteStreams.toByteArray(dicomFileStream))
          } finally {
            Closeables.close(fileStream, true)
            Closeables.close(dicomFileStream, true)
          }
        case (other, _) =>
          throw QueryExecutionErrors.unsupportedFieldNameError(other)
      }

      Iterator.single(writer.getRow)
    }
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType
  ): OutputWriterFactory = {
    new OutputWriterFactory() {

      override def getFileExtension(context: TaskAttemptContext): String =
        ".dcm"

      def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext
      ): OutputWriter = new DicomOutputWriter(path, dataSchema, context)

    }
  }

}
