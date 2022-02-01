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

import ai.kaiko.dicom.DicomDeidElem
import ai.kaiko.dicom.DicomDeidentifyDictionary
import ai.kaiko.dicom.DicomStandardDictionary
import ai.kaiko.dicom.DicomStdElem
import ai.kaiko.spark.dicom.deidentifier.options.DeidOption
import ai.kaiko.spark.dicom.deidentifier.options._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.dcm4che3.data._

case class DicomDeidentifierConfig(
    retainUids: Boolean = false,
    retainDevId: Boolean = false,
    retainInstId: Boolean = false,
    retainPatChars: Boolean = false,
    retainLongFullDates: Boolean = false,
    retainLongModifDates: Boolean = false,
    cleanDesc: Boolean = false,
    cleanStructCont: Boolean = false,
    cleanGraph: Boolean = false
)
object DicomDeidentifier {

  /** Gets the correct action according to the deidElem, vr, and confih
    *
    * @param deidElem
    *   entry in the table from the DICOM standard that specifies elements to
    *   de-id
    *   https://dicom.nema.org/medical/dicom/current/output/html/part15.html#table_E.1-1
    * @param vr
    *   value represenation of the deidElem
    * @param config
    *   configuration settings to be used in de-identification
    */
  def getAction(
      deidElem: DicomDeidElem,
      vr: VR,
      config: DicomDeidentifierConfig
  ): DeidAction = {

    DeidOption.values
      .map({ deidOpt =>
        val (configSetting, action) = deidOpt match {
          case CleanGraph => (config.cleanGraph, deidElem.cleanGraphAction)
          case CleanStructCont =>
            (config.cleanStructCont, deidElem.cleanStructContAction)
          case CleanDesc => (config.cleanDesc, deidElem.cleanDescAction)
          case RetainLongModifDates =>
            (config.retainLongModifDates, deidElem.retainLongModifDatesAction)
          case RetainLongFullDates =>
            (config.retainLongFullDates, deidElem.retainLongFullDatesAction)
          case RetainPatChars =>
            (config.retainPatChars, deidElem.retainPatCharsAction)
          case RetainInstId =>
            (config.retainInstId, deidElem.retainInstIdAction)
          case RetainDevId => (config.retainDevId, deidElem.retainDevIdAction)
          case RetainUids  => (config.retainUids, deidElem.retainUidsAction)
        }
        if (configSetting) action else None
      })
      .find(_.isDefined)
      .flatten // take config option with the highest priority
      .getOrElse(deidElem.action) match { // no config options found. Take default action.
      case "Z" | "Z/D" => Empty(DicomDeidentifyDictionary.getEmptyValue(vr))
      case "D" | "D/X" => Dummify(DicomDeidentifyDictionary.getDummyValue(vr))
      case "C"         => Clean()
      case "U"         => Pseudonymize()
      case "X" | "X/Z" | "X/D" | "X/Z/D" | "X/Z/U*" => Drop()
      case "K"                                      => Keep()
    }
  }

  /** De-identifies a Dataframe that was loaded with the `dicomFile` format See:
    * https://dicom.nema.org/medical/dicom/current/output/html/part15.html#chapter_E
    *
    * @param dataframe
    *   columns need to be keywords as defined in the DICOM standard
    * @param config
    *   configuration settings to be used in de-identification
    */
  def deidentify(
      dataframe: DataFrame,
      config: DicomDeidentifierConfig = DicomDeidentifierConfig()
  ): DataFrame = {
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
          getAction(deidElem, vr, config).makeDeidentifiedColumn(keyword)
        case (keyword, _, _) => Some(col(keyword))
      })
      .collect({ case Some(column) =>
        column
      })

    dataframe.select(columns: _*)
  }
}
