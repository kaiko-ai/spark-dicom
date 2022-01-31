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
import ai.kaiko.spark.dicom.deidentifier.options._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class DicomDeidentifierOptions(
    retainUids: Boolean = false,
    retainDevId: Boolean = false,
    retainInstId: Boolean = false,
    retainPatChars: Boolean = false,
    retainLongFullDates: Boolean = false,
    retainLongModifDates: Boolean = false,
    cleanDesc: Boolean = false,
    cleanStructCont: Boolean = false,
    cleanGraph: Boolean = false
) {

  val prioritizedOptions: Seq[DeidOption] = {
    getDeidOption(this.cleanGraph, CleanGraph()) ++
      getDeidOption(this.cleanStructCont, CleanStructCont()) ++
      getDeidOption(this.cleanDesc, CleanDesc()) ++
      getDeidOption(this.retainLongModifDates, RetainLongModifDates()) ++
      getDeidOption(this.retainLongFullDates, RetainLongFullDates()) ++
      getDeidOption(this.retainPatChars, RetainPatChars()) ++
      getDeidOption(this.retainInstId, RetainInstId()) ++
      getDeidOption(this.retainDevId, RetainDevId()) ++
      getDeidOption(this.retainUids, RetainUids())
  }

  def getDeidOption(flag: Boolean, option: DeidOption) = {
    if (flag) option :: Nil else Nil
  }

  def getAction(deid: DicomDeidElem): DeidAction = {
    prioritizedOptions
      .map(_.getOptionAction(deid))
      .collect({ case Some(action) =>
        action
      })
      .headOption
      .getOrElse(deid.action) match {
      case "Z" | "Z/D"                              => Empty()
      case "D" | "D/X"                              => Dummify()
      case "C"                                      => Clean()
      case "U"                                      => Pseudonymize()
      case "X" | "X/Z" | "X/D" | "X/Z/D" | "X/Z/U*" => Drop()
      case "K"                                      => Keep()
    }
  }
}

object DicomDeidentifier {

  /** De-identifies a Dataframe that was loaded with the `dicomFile` format See:
    * https://dicom.nema.org/medical/dicom/current/output/html/part15.html#chapter_E
    *
    * @param dataframe
    *   columns need to be keywords as defined in the DICOM standard
    */
  def deidentify(
      dataframe: DataFrame,
      options: DicomDeidentifierOptions = DicomDeidentifierOptions()
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
          options.getAction(deidElem).deidentify(keyword, vr)
        case (keyword, _, _) => Some(col(keyword))
      })
      .collect({ case Some(column) =>
        column
      })

    dataframe.select(columns: _*)
  }
}
