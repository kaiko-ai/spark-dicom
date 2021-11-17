package ai.kaiko.spark.dicom

import ai.kaiko.dicom.DicomFile
import ai.kaiko.dicom.DicomHelper
import ai.kaiko.dicom.StringValue
import ai.kaiko.dicom.UnsupportedValue
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
import org.dcm4che3.data.Keyword
import org.dcm4che3.data.Tag
import org.dcm4che3.data.UID
import org.dcm4che3.data.VR
import org.dcm4che3.io.DicomInputStream
import org.dcm4che3.io.DicomOutputStream

import java.net.URI

object DicomFileReader {
  def inferSchema(conf: Configuration, file: FileStatus): StructType = {
    val fs = file.getPath.getFileSystem(conf)
    val fileStream = fs.open(file.getPath)
    val dicomInputStream = new DicomInputStream(fileStream)
    val dicomFile = DicomFile.readDicomFile(dicomInputStream)
    val fields = (dicomFile.keywords zip dicomFile.vrs)
      .map { case (kw, vr) =>
        DicomHelper.maybeBuildSparkStructFieldFrom(kw, vr)
      }
      .filter((mb) => mb.isDefined)
      .map((mb) => mb.get)
    // add file attributes to default schema
    StructType(DicomFileFormat.DEFAULT_SCHEMA.union(StructType(fields)))
  }

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

    val fileStream = fs.open(status.getPath)
    val dicomInputStream = new DicomInputStream(fileStream)
    val dicomFile = DicomFile.readDicomFile(dicomInputStream)

    // TODO filters

    val writer = new UnsafeRowWriter(requiredSchema.length)
    writer.resetRowWriter()
    requiredSchema.fieldNames.zipWithIndex.foreach {
      case (DicomFileFormat.PATH, i) =>
        writer.write(i, UTF8String.fromString(status.getPath.toString))
      case (DicomFileFormat.CONTENT, i) =>
        val fileStream = fs.open(status.getPath)
        val dicomInputStream = new DicomInputStream(fileStream)
        try {
          writer.write(i, ByteStreams.toByteArray(dicomInputStream))
        } finally {
          Closeables.close(fileStream, true)
          Closeables.close(dicomInputStream, true)
        }
      case (keyword, i) => {
        val keywordIndex = dicomFile.keywords.indexOf(keyword)
        if (keywordIndex == -1) {
          throw QueryExecutionErrors.unsupportedFieldNameError(keyword)
        }
        dicomFile.values(i) match {
          case v: StringValue => writer.write(i, UTF8String.fromString(v.value))
          case v: UnsupportedValue =>
            throw new Exception(
              "Unsupported value type for keyword " ++ keyword
            )
        }
      }

    }

    Iterator.single(writer.getRow)
  }
}
