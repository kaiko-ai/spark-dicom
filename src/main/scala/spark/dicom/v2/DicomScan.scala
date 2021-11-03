package spark.dicom.v2

import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.{util => ju}
import org.apache.spark.sql.connector.read.Batch

class DicomScan(
    val schema: StructType,
    val properties: ju.Map[String, String],
    val options: CaseInsensitiveStringMap
) extends Scan {

  override def readSchema(): StructType = schema

  override def toBatch(): Batch = new DicomBatch(schema, properties, options)
}
