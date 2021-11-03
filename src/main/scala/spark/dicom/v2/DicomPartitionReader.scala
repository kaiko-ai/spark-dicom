package spark.dicom.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

class DicomPartitionReader(
    inputPartition: DicomInputPartition,
    schema: StructType,
    fileName: String
) extends PartitionReader[InternalRow] {

  override def close(): Unit = ???

  override def next(): Boolean = ???

  override def get(): InternalRow = ???

}
