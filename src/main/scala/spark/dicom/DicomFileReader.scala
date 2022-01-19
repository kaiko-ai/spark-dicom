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
package ai.kaiko.spark.dicom

import ai.kaiko.dicom.DicomStandardDictionary
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.dcm4che3.data
import org.dcm4che3.io.DicomInputStream

import java.net.URI
import scala.util.Try

object DicomFileReader {
  val FIELD_NAME_PATH = "path"
  val FIELD_NAME_ISDICOM = "isDicom"
  val FIELD_NAME_KEYWORDS = "keywords"
  val FIELD_NAME_VRS = "vrs"
  val METADATA_FIELDS = Array(
    StructField(FIELD_NAME_PATH, StringType, false),
    StructField(FIELD_NAME_ISDICOM, BooleanType, false),
    StructField(FIELD_NAME_KEYWORDS, ArrayType(StringType, false), false),
    StructField(FIELD_NAME_VRS, MapType(StringType, StringType), false)
  )

  def readDicomFile(
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      broadcastedHadoopConf: Broadcast[SerializableConfiguration],
      file: PartitionedFile
  ): Iterator[InternalRow] = {
    val path = new Path(new URI(file.filePath))
    val fs = path.getFileSystem(broadcastedHadoopConf.value.value)
    val status = fs.getFileStatus(path)

    val readPixelData = requiredSchema.fieldNames.contains(
      data.Keyword.valueOf(data.Tag.PixelData)
    )

    val fileStream = fs.open(status.getPath)
    val tryAttrs = Try({
      val dicomInputStream = new DicomInputStream(fileStream)
      if (readPixelData) dicomInputStream.readDataset
      else dicomInputStream.readDatasetUntilPixelData
    })

    // TODO filters
    val mutableRow = new GenericInternalRow(requiredSchema.size)

    requiredSchema.fieldNames.zipWithIndex.foreach {
      // meta fields
      case (FIELD_NAME_PATH, i) => {
        val writer = InternalRow.getWriter(i, StringType)
        writer(mutableRow, UTF8String.fromString(status.getPath.toString))
      }
      case (FIELD_NAME_ISDICOM, i) => {
        val writer = InternalRow.getWriter(i, BooleanType)
        val isDicom = tryAttrs.isSuccess
        writer(mutableRow, isDicom)
      }
      case (FIELD_NAME_KEYWORDS, i) => {
        tryAttrs.map(attrs => {
          val keywords = attrs.tags
            .map(tag => DicomStandardDictionary.tagMap.get(tag))
            .collect { case Some(stdElem) => stdElem.keyword }
          val writer = InternalRow.getWriter(i, ArrayType(StringType, false))
          writer(
            mutableRow,
            ArrayData.toArrayData(keywords.map(UTF8String.fromString))
          )
        })
      }
      case (FIELD_NAME_VRS, i) => {
        tryAttrs.map(attrs => {
          val keywordToVr = attrs.tags
            .map(tag => DicomStandardDictionary.tagMap.get(tag))
            .collect { case Some(stdElem) =>
              stdElem.keyword -> attrs.getVR(stdElem.tag)
            }
            .toMap
          val writer = InternalRow.getWriter(i, MapType(StringType, StringType))
          writer(
            mutableRow,
            ArrayBasedMapData(
              keywordToVr,
              keyConverter =
                (v: Any) => UTF8String.fromString(v.asInstanceOf[String]),
              valueConverter =
                (v: Any) => UTF8String.fromString(v.asInstanceOf[data.VR].name)
            )
          )
        })
      }
      // any other requested field should be a DICOM keyword
      case (keyword, i) => {
        tryAttrs.map(attrs => {
          DicomStandardDictionary.keywordMap.get(keyword) match {
            case None =>
              throw QueryExecutionErrors.unsupportedFieldNameError(keyword)
            case Some(stdElem) => {
              val sparkMapper = stdElem.vr.toOption.map {
                DicomSparkMapper.from
              } getOrElse DicomSparkMapper.DEFAULT_MAPPER
              val writer = InternalRow.getWriter(i, sparkMapper.sparkDataType)
              val value = sparkMapper.reader(attrs, stdElem.tag)
              writer(mutableRow, value)
            }
          }
        })
      }
    }
    Iterator.single(mutableRow)
  }
}
