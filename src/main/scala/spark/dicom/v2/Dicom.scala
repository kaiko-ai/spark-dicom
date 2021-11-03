package spark.dicom.v2

import ai.kaiko.spark.dicom.DicomFileFormat
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableProvider
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.{util => ju}

class Dicom extends TableProvider {

  override def inferSchema(
      options: CaseInsensitiveStringMap
  ): StructType = DicomFileFormat.SCHEMA

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: ju.Map[String, String]
  ): Table = new DicomTable(schema, properties)

}
