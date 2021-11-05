package ai.kaiko.spark.dicom.v2

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.sql.execution.datasources.CodecStreams
import org.apache.hadoop.fs.Path
import java.net.URI
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.unsafe.types.UTF8String
import org.dcm4che3.io.DicomInputStream
import com.google.common.io.ByteStreams
import com.google.common.io.Closeables
import org.apache.spark.sql.errors.QueryExecutionErrors
import ai.kaiko.spark.dicom.DicomFileFormat

final case class DicomPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    filters: Seq[Filter]
) extends FilePartitionReaderFactory {

  override def buildReader(
      file: PartitionedFile
  ): PartitionReader[InternalRow] = {
    val conf = broadcastedConf.value.value
    val path = new Path(new URI(file.filePath))
    val fs = path.getFileSystem(conf)
    val stream = CodecStreams.createInputStreamWithCloseResource(conf, path)

    val status = fs.getFileStatus(path)

    val rowWriter = new UnsafeRowWriter(readDataSchema.length)
    rowWriter.resetRowWriter()
    readDataSchema.fieldNames.zipWithIndex.foreach {
      case (DicomFileFormat.PATH, i) =>
        rowWriter.write(i, UTF8String.fromString(status.getPath.toString))
      case (DicomFileFormat.LENGTH, i) => rowWriter.write(i, status.getLen)
      case (DicomFileFormat.CONTENT, i) =>
        val fileStream = fs.open(status.getPath);
        val dicomFileStream = new DicomInputStream(fileStream);
        try {
          rowWriter.write(i, ByteStreams.toByteArray(dicomFileStream))
        } finally {
          Closeables.close(dicomFileStream, true)
        }
      case (other, _) =>
        throw QueryExecutionErrors.unsupportedFieldNameError(other)
    }

    val iter = Iterator.single(rowWriter.getRow)
    val fileReader = new PartitionReaderFromIterator[InternalRow](iter)
    new PartitionReaderWithPartitionValues(
      fileReader,
      readDataSchema,
      partitionSchema,
      file.partitionValues
    )
  }
}
