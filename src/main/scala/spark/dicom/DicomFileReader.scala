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
import ai.kaiko.dicom.json.DicomJson
import org.apache.commons.io.IOUtils
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
import org.dcm4che3.io.DicomInputStream
import org.dcm4che3.{data => dcmData}

import java.net.URI
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import collection.JavaConverters._

sealed trait FieldWriteResult
sealed case class FieldWritten() extends FieldWriteResult
sealed case class FieldIgnored() extends FieldWriteResult
sealed case class FieldSkipped() extends FieldWriteResult
sealed case class ValueParseFailure(
    fieldName: String,
    index: Int,
    error: Throwable
) extends FieldWriteResult

object DicomFileReader {
  // metadata fields
  val FIELD_NAME_PATH = "path"
  val FIELD_NAME_ISDICOM = "isDicom"
  val FIELD_NAME_KEYWORDS = "keywords"
  val FIELD_NAME_VRS = "vrs"
  val FIELD_NAME_ERRORS = "errors"
  val METADATA_FIELDS = Array(
    StructField(FIELD_NAME_PATH, StringType, false),
    StructField(FIELD_NAME_ISDICOM, BooleanType, false),
    StructField(FIELD_NAME_KEYWORDS, ArrayType(StringType, false), false),
    StructField(FIELD_NAME_VRS, MapType(StringType, StringType), false),
    StructField(FIELD_NAME_ERRORS, MapType(StringType, StringType), false)
  )
  // other fields
  val FIELD_NAME_CONTENT = "content"
  val FIELD_NAME_PRIVATETAGS = "privateTagsJson"

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
      dcmData.Keyword.valueOf(dcmData.Tag.PixelData)
    )

    val tryAttrs = Try({
      val fileStream = fs.open(status.getPath)
      val dicomInputStream = new DicomInputStream(fileStream)
      if (readPixelData) dicomInputStream.readDataset
      else dicomInputStream.readDatasetUntilPixelData
    })

    // TODO filters
    val mutableRow = new GenericInternalRow(requiredSchema.size)

    val rowWriteResults: Array[FieldWriteResult] =
      requiredSchema.fieldNames.zipWithIndex.map {
        // meta fields
        case (FIELD_NAME_PATH, i) => {
          val writer = InternalRow.getWriter(i, StringType)
          writer(mutableRow, UTF8String.fromString(status.getPath.toString))
          FieldWritten()
        }
        case (FIELD_NAME_ISDICOM, i) => {
          val writer = InternalRow.getWriter(i, BooleanType)
          val isDicom = tryAttrs.isSuccess
          writer(mutableRow, isDicom)
          FieldWritten()
        }
        case (FIELD_NAME_KEYWORDS, i) => {
          tryAttrs
            .map(attrs => {
              val keywords = attrs.tags
                .map(tag => DicomStandardDictionary.tagMap.get(tag))
                .collect { case Some(stdElem) => stdElem.keyword }
              val writer =
                InternalRow.getWriter(i, ArrayType(StringType, false))
              writer(
                mutableRow,
                ArrayData.toArrayData(keywords.map(UTF8String.fromString))
              )
              FieldWritten()
            })
            .getOrElse(FieldSkipped())
        }
        case (FIELD_NAME_VRS, i) => {
          tryAttrs
            .map(attrs => {
              val keywordToVr = attrs.tags
                .map(tag => DicomStandardDictionary.tagMap.get(tag))
                .collect { case Some(stdElem) =>
                  stdElem.keyword -> attrs.getVR(stdElem.tag)
                }
                .toMap
              val writer =
                InternalRow.getWriter(i, MapType(StringType, StringType))
              writer(
                mutableRow,
                ArrayBasedMapData(
                  keywordToVr,
                  keyConverter =
                    (v: Any) => UTF8String.fromString(v.asInstanceOf[String]),
                  valueConverter = (v: Any) =>
                    UTF8String.fromString(v.asInstanceOf[dcmData.VR].name)
                )
              )
              FieldWritten()
            })
            .getOrElse(FieldSkipped())
        }
        case (FIELD_NAME_ERRORS, _) => {
          // skip, we'll write it later
          FieldIgnored()
        }
        case (FIELD_NAME_CONTENT, i) => {
          val fileStream = fs.open(status.getPath)
          val bytes = IOUtils.toByteArray(fileStream)
          val writer = InternalRow.getWriter(i, BinaryType)
          writer(mutableRow, bytes)
          FieldWritten()
        }
        case (FIELD_NAME_PRIVATETAGS, i) => {
          tryAttrs
            .map(attrs => {
              // list private tags
              // they are the tags in attrs which don't match any tag in the standard dicom dictionary
              val privateTags: Array[Int] =
                attrs.tags
                  .filter(tag =>
                    DicomStandardDictionary.elements.find(_.tag == tag).isEmpty
                  )
              // build attrs of only those tags
              val privateAttrs: dcmData.Attributes = {
                val privateAttrs = new dcmData.Attributes(privateTags.length)
                privateTags.foreach(tag => {
                  Option(attrs.getPrivateCreator(tag)) match {
                    case Some(privateCreator) => {
                      privateAttrs.addSelected(attrs, privateCreator, tag)
                    }
                    case None => privateAttrs.addSelected(attrs, tag)
                  }
                })
                privateAttrs
              }
              // write out to JSON
              val jsonStr: String = {
                val jsonObject = DicomJson.attrs2jsonobject(privateAttrs)
                val sw = new java.io.StringWriter
                val jwf = javax.json.Json.createWriterFactory(Map.empty.asJava)
                val jw = jwf.createWriter(sw)
                jw.write(jsonObject)
                jw.close
                sw.toString
              }
              // write JSON string to row
              val writer = InternalRow.getWriter(i, StringType)
              writer(mutableRow, UTF8String.fromString(jsonStr))
              FieldWritten()
            })
            .getOrElse(FieldSkipped())
        }
        // any other requested field should be a DICOM keyword
        case (keyword, i) => {
          DicomStandardDictionary.keywordMap.get(keyword) match {
            case None =>
              throw QueryExecutionErrors.unsupportedFieldNameError(keyword)
            case Some(stdElem) => {
              tryAttrs
                .map(attrs => {
                  val sparkMapper = stdElem.vr.toOption.map {
                    DicomSparkMapper.from
                  } getOrElse DicomSparkMapper.DEFAULT_MAPPER
                  val writer =
                    InternalRow.getWriter(i, sparkMapper.sparkDataType)
                  val tryValue = Try({ sparkMapper.reader(attrs, stdElem.tag) })
                  tryValue match {
                    case Failure(exception) =>
                      ValueParseFailure(keyword, i, exception)
                    case Success(value) => {
                      writer(mutableRow, value)
                      FieldWritten()
                    }
                  }
                })
                .getOrElse(FieldSkipped())
            }
          }
        }
      }

    // if requested, write errors
    requiredSchema.fields.zipWithIndex
      .find { case (field, i) => field.name equals FIELD_NAME_ERRORS }
      .map({
        case (_, i) => {
          val mapKeywordToError: Map[String, String] =
            rowWriteResults.collect {
              case ValueParseFailure(fieldName, _, error) =>
                fieldName -> error.toString
            }.toMap
          val writer = InternalRow.getWriter(i, MapType(StringType, StringType))
          writer(
            mutableRow,
            ArrayBasedMapData(
              mapKeywordToError,
              keyConverter =
                (v: Any) => UTF8String.fromString(v.asInstanceOf[String]),
              valueConverter =
                (v: Any) => UTF8String.fromString(v.asInstanceOf[String])
            )
          )
        }
      })

    Iterator.single(mutableRow)
  }
}
