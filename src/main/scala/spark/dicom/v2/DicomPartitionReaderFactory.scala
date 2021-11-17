package ai.kaiko.spark.dicom.v2

import ai.kaiko.spark.dicom.DicomFileFormat
import ai.kaiko.spark.dicom.DicomFileReader
import com.google.common.io.ByteStreams
import com.google.common.io.Closeables
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.CodecStreams
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.dcm4che3.io.DicomInputStream

import java.net.URI

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
    val iter = DicomFileReader.readDicomFile(
      dataSchema,
      partitionSchema,
      readDataSchema,
      filters,
      broadcastedConf,
      file
    )
    val fileReader = new PartitionReaderFromIterator[InternalRow](iter)
    new PartitionReaderWithPartitionValues(
      fileReader,
      readDataSchema,
      partitionSchema,
      file.partitionValues
    )
  }
}
