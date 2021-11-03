package spark.dicom.v2

import org.apache.spark.sql.connector.read.Batch
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.{util => ju}

class DicomBatch(
    val schema: StructType,
    val properties: ju.Map[String, String],
    val options: CaseInsensitiveStringMap
) extends Batch {

  var filename = options.get("fileName")

  override def planInputPartitions(): Array[InputPartition] = Array(
    new DicomInputPartition()
  )

  override def createReaderFactory(): PartitionReaderFactory = ???

}
