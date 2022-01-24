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

object DicomDataSource {
  val OPTION_WITHPIXELDATA = "includePixelData"
  val OPTION_WITHCONTENT = "includeContent"

  def schema(withPixelData: Boolean = false, withContent: Boolean = false) = {
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

    val otherFields: Array[StructField] =
      if (withContent) Array(StructField("content", BinaryType))
      else Array.empty

    new StructType(metadataFields ++ selectedFields ++ otherFields)
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
