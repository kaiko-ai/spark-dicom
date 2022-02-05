// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package ai.kaiko.spark.dicom.v2

import ai.kaiko.dicom.DicomStandardDictionary
import ai.kaiko.spark.dicom.DicomFileReader
import ai.kaiko.spark.dicom.DicomSparkMapper
import ai.kaiko.spark.dicom.v1.DicomFileFormat
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.dcm4che3.data.Keyword
import org.dcm4che3.data.Tag
import org.dcm4che3.data.VR

import scala.util.Try

object DicomDataSource {
  val OPTION_INCLUDEPIXELDATA = "includePixelData"
  val OPTION_INCLUDECONTENT = "includeContent"
  val OPTION_INCLUDEPRIVATETAGS = "includePrivateTags"

  lazy val defaultFields = {
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
    fields
  }

  def schema(
      includePixelData: Boolean,
      includeContent: Boolean,
      includePrivateTags: Boolean
  ): StructType = {
    // for now, only PixelData is a field in standard DICOM that can be removed
    val selectedFields: Array[StructField] =
      (
        if (includePixelData) defaultFields
        else defaultFields.filter(p => p.name != Keyword.valueOf(Tag.PixelData))
      )

    // build optional fields when included
    val optionalFields: Array[StructField] = Array(
      if (includeContent)
        Some(
          StructField(DicomFileReader.FIELD_NAME_CONTENT, BinaryType)
        )
      else None,
      if (includePrivateTags)
        Some(StructField(DicomFileReader.FIELD_NAME_PRIVATETAGS, StringType))
      else None
    ).collect { case Some(value) => value }

    new StructType(
      DicomFileReader.METADATA_FIELDS ++ selectedFields ++ optionalFields
    )
  }

  def schema(options: Map[String, String]): StructType = {
    def getBoolOpt(key: String): Boolean = options
      .get(key.toLowerCase)
      .flatMap(b => Try(b.toBoolean).toOption)
      .getOrElse(false)

    val includePixelData: Boolean = getBoolOpt(
      DicomDataSource.OPTION_INCLUDEPIXELDATA
    )
    val includeContent: Boolean = getBoolOpt(
      DicomDataSource.OPTION_INCLUDECONTENT.toLowerCase
    )
    val includePrivateTags: Boolean = getBoolOpt(
      DicomDataSource.OPTION_INCLUDEPRIVATETAGS.toLowerCase
    )

    DicomDataSource.schema(
      includePixelData = includePixelData,
      includeContent = includeContent,
      includePrivateTags = includePrivateTags
    )
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
