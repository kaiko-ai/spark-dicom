package ai.kaiko.spark.dicom.v2

import ai.kaiko.spark.dicom.DicomFileFormat
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableProvider
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.{util => ju}
import org.apache.spark.sql.execution.datasources.FileFormat

class DicomDataSourceV2 extends FileDataSourceV2 {

  override def fallbackFileFormat: Class[_ <: FileFormat] =
    classOf[DicomFileFormat]

  override def shortName(): String = "dicom"

  override protected def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    DicomTable(
      tableName,
      sparkSession,
      optionsWithoutPaths,
      paths,
      None,
      fallbackFileFormat
    )
  }

  override def getTable(
      options: CaseInsensitiveStringMap,
      schema: StructType
  ): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    DicomTable(
      tableName,
      sparkSession,
      optionsWithoutPaths,
      paths,
      Some(schema),
      fallbackFileFormat
    )
  }

}
