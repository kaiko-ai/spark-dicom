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

import ai.kaiko.dicom.json.DicomJson
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.dcm4che3.data._

import java.time.LocalDate
import java.time.LocalTime
import java.time.format.DateTimeFormatter

/** @param sparkDataType
  *   Spark DataType of the output of the reader function
  * @param reader
  *   Function that retrieves the proper JVM value from an element in a
  *   [[org.dcm4che3.data.Attributes]] to be stored in an
  *   [[org.apache.spark.sql.catalyst.InternalRow]]
  */
case class DicomSparkMapper(
    sparkDataType: DataType,
    reader: (Attributes, Int) => Any
)

object DicomSparkMapper {
  lazy val DEFAULT_MAPPER = DicomSparkMapper(
    sparkDataType = BinaryType,
    reader = _.getBytes(_)
  )

  lazy val OB_OW_MAPPER = DicomSparkMapper(
    sparkDataType = BinaryType,
    reader = _.getBytes(_)
  )

  def from(vr: VR): DicomSparkMapper = {
    import VR._
    vr match {
      case AE | AS | AT | CS | DS | DT | IS | LO | LT | SH | ST | UC | UI | UR |
          UT =>
        DicomSparkMapper(
          sparkDataType = StringType,
          reader = (attrs, tag) =>
            UTF8String.fromString(Option(attrs.getString(tag)).getOrElse(""))
        )
      case PN =>
        DicomSparkMapper(
          sparkDataType = new StructType(
            Array(
              new StructField("Alphabetic", StringType, true),
              new StructField("Ideographic", StringType, true),
              new StructField("Phonetic", StringType, true)
            )
          ),
          reader = (attrs, tag) => {
            val personName = new PersonName(attrs.getString(tag), true)
            InternalRow(
              UTF8String.fromString(
                Option(personName.toString(PersonName.Group.Alphabetic, true))
                  .getOrElse("")
              ),
              UTF8String.fromString(
                Option(
                  personName.toString(PersonName.Group.Ideographic, true)
                ).getOrElse("")
              ),
              UTF8String.fromString(
                Option(personName.toString(PersonName.Group.Phonetic, true))
                  .getOrElse("")
              )
            )
          }
        )
      case FL | FD =>
        DicomSparkMapper(
          sparkDataType = ArrayType(DoubleType, false),
          reader = (attrs, tag) =>
            ArrayData.toArrayData(
              Option(attrs.getDoubles(tag)).getOrElse(Array.empty)
            )
        )
      case SL | SS | US | UL =>
        DicomSparkMapper(
          sparkDataType = ArrayType(IntegerType, false),
          reader = (attrs, tag) =>
            ArrayData.toArrayData(
              Option(attrs.getInts(tag)).getOrElse(Array.empty)
            )
        )
      case SV | UV =>
        DicomSparkMapper(
          sparkDataType = ArrayType(LongType, false),
          reader = (attrs, tag) =>
            ArrayData.toArrayData(
              Option(attrs.getLongs(tag)).getOrElse(Array.empty)
            )
        )
      case DA =>
        DicomSparkMapper(
          sparkDataType = StringType,
          reader = (attrs, tag) =>
            UTF8String.fromString(
              Option(attrs.getString(tag))
                .map(
                  LocalDate
                    .parse(
                      _,
                      DateTimeFormatter
                        .ofPattern("yyyyMMdd")
                    )
                    .format(DateTimeFormatter.ISO_LOCAL_DATE)
                )
                .getOrElse("")
            )
        )
      case TM =>
        DicomSparkMapper(
          sparkDataType = StringType,
          reader = (attrs, tag) =>
            UTF8String.fromString(
              Option(attrs.getString(tag))
                .map(
                  LocalTime
                    .parse(
                      _,
                      DateTimeFormatter
                        .ofPattern(
                          "HHmmss[.SSSSSS][.SSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]"
                        )
                    )
                    .format(DateTimeFormatter.ISO_LOCAL_TIME)
                )
                .getOrElse("")
            )
        )
      case SQ =>
        DicomSparkMapper(
          sparkDataType = StringType,
          reader = (attrs, tag) =>
            UTF8String.fromString(
              Option(attrs.getSequence(tag))
                .map(seq => {
                  val ja = DicomJson.seq2jsonarray(seq)
                  DicomJson.json2string(ja)
                })
                .getOrElse("")
            )
        )
      case OB | OW => OB_OW_MAPPER
      // map others to binary for the time being
      case _ => DEFAULT_MAPPER
    }
  }
}
