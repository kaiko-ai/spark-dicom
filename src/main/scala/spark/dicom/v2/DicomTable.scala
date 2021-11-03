package spark.dicom.v2

import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.{util => ju}
import scala.collection.JavaConverters
import scala.collection.mutable

class DicomTable(schema: StructType, properties: ju.Map[String, String])
    extends SupportsRead {

  override def name(): String = "dicom_table"

  override def schema(): StructType = schema

  override def capabilities(): ju.Set[TableCapability] =
    JavaConverters.setAsJavaSet(Set(TableCapability.BATCH_READ))

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new DicomScanBuilder(schema, properties, options)

}
