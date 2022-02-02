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

import ai.kaiko.dicom.ActionCode
import ai.kaiko.dicom.DicomDeidElem
import ai.kaiko.dicom.DicomDeidentifyDictionary
import ai.kaiko.dicom.DicomStandardDictionary
import ai.kaiko.dicom.DicomStdElem
import ai.kaiko.spark.dicom.deidentifier.options.DeidOption
import ai.kaiko.spark.dicom.deidentifier.options._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.dcm4che3.data._

object DicomDeidentifier {

  /** Gets the correct action according to the deidElem, vr, and config
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
      config: Map[DeidOption, Boolean]
  ): DeidAction = {

    DeidOption.values
      .map({ deidOpt =>
        lazy val action = deidOpt match {
          case CleanGraph           => deidElem.cleanGraphAction
          case CleanStructCont      => deidElem.cleanStructContAction
          case CleanDesc            => deidElem.cleanDescAction
          case RetainLongModifDates => deidElem.retainLongModifDatesAction
          case RetainLongFullDates  => deidElem.retainLongFullDatesAction
          case RetainPatChars       => deidElem.retainPatCharsAction
          case RetainInstId         => deidElem.retainInstIdAction
          case RetainDevId          => deidElem.retainDevIdAction
          case RetainUids           => deidElem.retainUidsAction
        }
        if (config.get(deidOpt).getOrElse(false)) action else None
      })
      // take config option with the highest priority
      .find(_.isDefined)
      .flatten
      // no config options found. Take default action
      .getOrElse(deidElem.action) match {
      case ActionCode.Z => Empty(DicomDeidentifyDictionary.getEmptyValue(vr))
      case ActionCode.D => Dummify(DicomDeidentifyDictionary.getDummyValue(vr))
      case ActionCode.C => Clean()
      case ActionCode.U => Pseudonymize()
      case ActionCode.X => Drop()
      case ActionCode.K => Keep()
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
      config: Map[DeidOption, Boolean] = Map.empty
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
