package ai.kaiko.spark.dicom.v2

import ai.kaiko.dicom.DicomStandardDictionary
import ai.kaiko.spark.dicom.DicomFileReader
import ai.kaiko.spark.dicom.DicomSparkMapper
import ai.kaiko.spark.dicom.v1.DicomFileFormat
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.dcm4che3.data.Keyword
import org.dcm4che3.data.Tag
import org.dcm4che3.data.VR

object DicomDataSource {
  val OPTION_WITHPIXELDATA = "includePixelData"

  def schema(withPixelData: Boolean) = {
    val fields = DicomStandardDictionary.elements
      .collect {
        case stdElem if stdElem.vr.isRight =>
          StructField(
            stdElem.keyword,
            DicomSparkMapper.from(stdElem.vr.right.get).sparkDataType,
            nullable = true
          )
        // PixelData is "OB or OW" in DICOM standard, but OB/OW should be well parsed by dcm4che
        case stdElem
            if stdElem.vr.left.toOption
              .map(vrStr => vrStr equals "OB or OW")
              .getOrElse(false) =>
          StructField(
            stdElem.keyword,
            DicomSparkMapper.from(VR.OB).sparkDataType,
            nullable = true
          )
      }
    val selectedFields =
      if (withPixelData) fields
      else fields.filter(p => p.name != Keyword.valueOf(Tag.PixelData))

    val metadataFields = DicomFileReader.METADATA_FIELDS

    new StructType(metadataFields ++ selectedFields)
  }
}

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
