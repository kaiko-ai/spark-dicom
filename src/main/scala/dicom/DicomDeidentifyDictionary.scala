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
package ai.kaiko.dicom

import ai.kaiko.spark.dicom.deidentifier.options._
import org.dcm4che3.data.Keyword
import org.dcm4che3.data.VR
import org.dcm4che3.data.VR._

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import scala.util.Success
import scala.util.Try
import scala.xml.XML

import ActionCode.ActionCode

object ActionCode extends Enumeration {
  type ActionCode = Value

  val D, Z, X, K, C, U = Value
}

case class DicomDeidElem(
    tag: Int,
    name: String,
    keyword: String,
    action: ActionCode,
    deidOptionToAction: Map[DeidOption, ActionCode]
)

object DicomDeidentifyDictionary {

  val DUMMY_DATE =
    LocalDate.of(1, 1, 1).format(DateTimeFormatter.ISO_LOCAL_DATE)
  val DUMMY_TIME =
    LocalTime.of(0, 0, 0, 0).format(DateTimeFormatter.ISO_LOCAL_TIME)
  val DUMMY_DATE_TIME = LocalDateTime
    .of(LocalDate.of(1, 1, 1), LocalTime.of(0, 0, 0, 0))
    .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
  val ZERO_STRING = "0"
  val ZERO_INT = 0
  val EMPTY_STRING = ""
  val DUMMY_STRING = "Anonymized"

  val DICOM_DEID_XML_DOC_FILEPATH =
    "/dicom/stdDoc/part15.xml"

  lazy val elements: Array[DicomDeidElem] = {
    val xmlResourceInputStream =
      Option(
        DicomDeidentifyDictionary.getClass.getResourceAsStream(
          DICOM_DEID_XML_DOC_FILEPATH
        )
      ).get
    val dicomDeidXmlDoc = XML.load(xmlResourceInputStream)
    // find relevant xml table holding dict
    ((dicomDeidXmlDoc \\ "table" filter (elem =>
      elem \@ "label" == "E.1-1"
    )) \ "tbody" \ "tr")
      // to Map entries
      .map(row => {
        // there is an invisible space in the texts, remove it
        val rowCellTexts = row \ "td" map (_.text.trim.replaceAll("​", ""))
        // we'll keep only elements with valid hexadecimal tag
        Try(
          Integer.parseInt(
            rowCellTexts(1)
              .replace("(", "")
              .replace(")", "")
              .replace(",", ""),
            16
          )
        ).map(intTag =>
          DicomDeidElem(
            tag = intTag,
            name = rowCellTexts(0),
            keyword = Keyword.valueOf(intTag),
            action = getActionCode(rowCellTexts(4)).getOrElse(ActionCode.X),
            deidOptionToAction = DeidOption.values
              .zip(
                Range(14, 5, -1).map(colIdx =>
                  getActionCode(rowCellTexts(colIdx))
                )
              )
              .collect({ case (option, Some(actionCode)) =>
                (option, actionCode)
              })
              .toMap
          )
        )
      })
      .collect { case Success(v) if v.name.nonEmpty => v }
      .toArray
  }

  lazy val keywordMap: Map[String, DicomDeidElem] =
    elements.map(deidElem => deidElem.keyword -> deidElem).toMap

  lazy val tagMap: Map[Int, DicomDeidElem] =
    elements.map(deidElem => deidElem.tag -> deidElem).toMap

  def getActionCode(action: String): Option[ActionCode.Value] = {
    action match {
      case "Z" | "Z/D"                              => Some(ActionCode.Z)
      case "D" | "D/X"                              => Some(ActionCode.D)
      case "C"                                      => Some(ActionCode.C)
      case "U"                                      => Some(ActionCode.U)
      case "X" | "X/Z" | "X/D" | "X/Z/D" | "X/Z/U*" => Some(ActionCode.X)
      case "K"                                      => Some(ActionCode.K)
      case ""                                       => None
    }
  }

  def getDummyValue(vr: VR): Option[Any] = {
    vr match {
      case LO | SH | PN | CS => Some(DUMMY_STRING)
      case DA                => Some(DUMMY_DATE)
      case TM                => Some(DUMMY_TIME)
      case DT                => Some(DUMMY_DATE_TIME)
      case IS                => Some(ZERO_STRING)
      case FD | FL | SS | US => Some(ZERO_INT)
      case ST                => Some(EMPTY_STRING)
      case _                 => None
    }
  }

  def getEmptyValue(vr: VR): Option[Any] = {
    vr match {
      case SH | PN | UI | LO | CS => Some(EMPTY_STRING)
      case DA                     => Some(DUMMY_DATE)
      case TM                     => Some(DUMMY_TIME)
      case UL                     => Some(ZERO_INT)
      case _                      => None
    }
  }
}
