package ai.kaiko.spark.dicom.v2

import ai.kaiko.spark.dicom.DicomFileReader
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

final case class DicomPartitionReaderFactory(
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    broadcastedConf: Broadcast[SerializableConfiguration]
) extends FilePartitionReaderFactory {

  override def buildReader(
      file: PartitionedFile
  ): PartitionReader[InternalRow] = {
    val iter = DicomFileReader.readDicomFile(
      dataSchema,
      readPartitionSchema,
      readDataSchema,
      Seq.empty,
      broadcastedConf,
      file
    )

    val fileReader = new PartitionReaderFromIterator[InternalRow](iter)
    new PartitionReaderWithPartitionValues(
      fileReader,
      readDataSchema,
      readPartitionSchema,
      file.partitionValues
    )
  }
}
