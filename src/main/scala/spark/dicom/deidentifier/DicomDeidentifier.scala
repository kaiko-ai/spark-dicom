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
package ai.kaiko.spark.dicom.deidentifier

import ai.kaiko.dicom.DicomDeidentifyDictionary
import ai.kaiko.dicom.DicomStandardDictionary
import ai.kaiko.dicom.DicomStdElem
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.dcm4che3.data._

sealed trait DeidAction {
  def deidentify(keyword: String, vr: VR): Option[Column]
}
sealed case class Empty() extends DeidAction {
  def deidentify(keyword: String, vr: VR): Option[Column] =
    DicomDeidentifyDictionary.getEmptyValue(vr) match {
      case Some(emptyVal) => Some(lit(emptyVal).as(keyword))
      case _              => None
    }
}
sealed case class Replace() extends DeidAction {
  def deidentify(keyword: String, vr: VR): Option[Column] =
    DicomDeidentifyDictionary.getDummyValue(vr) match {
      case Some(dummyVal) => Some(lit(dummyVal).as(keyword))
      case _              => None
    }
}
sealed case class Clean() extends DeidAction {
  def deidentify(keyword: String, vr: VR): Option[Column] = Some(
    lit("ToClean").as(keyword)
  )
}
sealed case class Pseudonymize() extends DeidAction {
  def deidentify(keyword: String, vr: VR): Option[Column] = Some(
    lit("ToPseudonymize").as(keyword)
  )
}

sealed case class Drop() extends DeidAction {
  def deidentify(keyword: String, vr: VR): Option[Column] = None
}

sealed case class Keep() extends DeidAction {
  def deidentify(keyword: String, vr: VR): Option[Column] = Some(col(keyword))
}

object DicomDeidentifier {

  def getAction(action: String): DeidAction = action match {
    case "Z" | "Z/D"                              => Empty()
    case "D" | "D/X"                              => Replace()
    case "C"                                      => Clean()
    case "U"                                      => Pseudonymize()
    case "X" | "X/Z" | "X/D" | "X/Z/D" | "X/Z/U*" => Drop()
    case "K"                                      => Keep()
  }

  /** De-identifies a Dataframe that was loaded with the `dicomFile` format
    * according to the Basic Confidentiality Profile. See:
    * https://dicom.nema.org/medical/dicom/current/output/html/part15.html#chapter_E
    *
    * @param dataframe
    *   columns need to be keywords as defined in the DICOM standard
    */
  def deidentify(dataframe: DataFrame): DataFrame = {

    val columns = dataframe.columns
      .map(keyword => {
        val stdElem = DicomStandardDictionary.keywordMap.get(keyword)
        val deidElem = DicomDeidentifyDictionary.keywordMap.get(keyword)
        (keyword, stdElem, deidElem)
      })
      .collect({
        case (
              keyword,
              Some(DicomStdElem(_, _, _, Right(vr), _, _)),
              Some(deidElem)
            ) =>
          getAction(deidElem.action).deidentify(keyword, vr)
        case (keyword, _, _) => Some(col(keyword))
      })
      .collect({ case Some(column) =>
        column
      })

    dataframe.select(columns: _*)
  }
}
