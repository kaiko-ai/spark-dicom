package ai.kaiko.spark.dicom

import ai.kaiko.spark.dicom.v2.DicomWrite
import com.google.common.io.ByteStreams
import com.google.common.io.Closeables
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
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

object DicomFileReader {
  def readDicomFile(
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      broadcastedHadoopConf: Broadcast[SerializableConfiguration],
      file: PartitionedFile
  ): Iterator[UnsafeRow] = {
    val path = new Path(new URI(file.filePath))
    val fs = path.getFileSystem(broadcastedHadoopConf.value.value)
    val status = fs.getFileStatus(path)

    // TODO filters

    val writer = new UnsafeRowWriter(requiredSchema.length)
    writer.resetRowWriter()
    requiredSchema.fieldNames.zipWithIndex.foreach {
      case (DicomFileFormat.PATH, i) =>
        writer.write(i, UTF8String.fromString(status.getPath.toString))
      case (DicomFileFormat.LENGTH, i) => writer.write(i, status.getLen)
      case (DicomFileFormat.CONTENT, i) =>
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
