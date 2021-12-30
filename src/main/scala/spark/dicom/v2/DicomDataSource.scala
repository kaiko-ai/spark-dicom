package ai.kaiko.spark.dicom.v2

import ai.kaiko.spark.dicom.DicomFileFormat
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DicomDataSource extends FileDataSourceV2 {

  override def fallbackFileFormat: Class[_ <: FileFormat] =
    classOf[DicomFileFormat]

  override def shortName(): String = "dicomFile"

  def getTable(
      options: CaseInsensitiveStringMap,
      optSchema: Option[StructType]
  ) = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    DicomTable(
      tableName,
      sparkSession,
      optionsWithoutPaths,
      paths,
      optSchema,
      fallbackFileFormat
    )
  }

  override protected def getTable(options: CaseInsensitiveStringMap): Table =
    getTable(options, None)

  override def getTable(
      options: CaseInsensitiveStringMap,
      schema: StructType
  ): Table = getTable(options, Some(schema))

}
