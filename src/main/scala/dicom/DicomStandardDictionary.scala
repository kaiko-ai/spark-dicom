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

import org.dcm4che3.data.VR

import scala.util.Try
import scala.xml.XML

case class DicomStdElem(
    tag: Int,
    name: String,
    keyword: String,
    vr: Either[String, VR],
    vm: String,
    note: String
)

object DicomStandardDictionary {
  val DICOM_STD_XML_DOC_FILEPATH =
    "/dicom/stdDoc/part06.xml"

  lazy val elements: Array[DicomStdElem] = {
    val xmlResourceInputStream =
      Option(
        DicomStandardDictionary.getClass.getResourceAsStream(
          DICOM_STD_XML_DOC_FILEPATH
        )
      ).get
    val dicomStdXmlDoc = XML.load(xmlResourceInputStream)
    // find relevant xml table holding dict
    ((dicomStdXmlDoc \\ "book" \ "chapter" filter (elem =>
      elem \@ "label" == "6" ||
        elem \@ "label" == "7" ||
        elem \@ "label" == "8" ||
        elem \@ "label" == "9"
    )) \ "table" \ "tbody" \ "tr")
      // to Map entries
      .map(row => {
        // there is an invisible space in the texts, remove it
        val rowCellTexts = row \ "td" map (_.text.trim.replaceAll("​", ""))
        // we'll keep only std elements with valid hexadecimal tag
        Try(
          Integer.parseInt(
            rowCellTexts(0)
              .replace("(", "")
              .replace(")", "")
              .replace(",", ""),
            16
          )
        ).toOption.map(intTag =>
          DicomStdElem(
            tag = intTag,
            name = rowCellTexts(1),
            keyword = rowCellTexts(2),
            vr = {
              val vrStr = rowCellTexts(3)
              Try(VR.valueOf(vrStr)).toOption.toRight(vrStr)
            },
            vm = rowCellTexts(4),
            note = rowCellTexts(5)
          )
        )
      })
      .collect { case Some(v) if v.keyword.nonEmpty => v }
      .toArray
  }

  lazy val keywordMap: Map[String, DicomStdElem] =
    elements.map(stdElem => stdElem.keyword -> stdElem).toMap

  lazy val tagMap: Map[Int, DicomStdElem] =
    elements.map(stdElem => stdElem.tag -> stdElem).toMap
}
