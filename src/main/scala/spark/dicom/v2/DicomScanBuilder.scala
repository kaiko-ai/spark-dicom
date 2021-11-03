package spark.dicom.v2

import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.{util => ju}

class DicomScanBuilder(
    val schema: StructType,
    val properties: ju.Map[String, String],
    val options: CaseInsensitiveStringMap
) extends ScanBuilder {

  override def build(): Scan = new DicomScan(schema, properties, options)

}
