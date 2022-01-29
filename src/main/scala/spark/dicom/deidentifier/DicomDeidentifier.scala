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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DicomDeidentifier {

  /** Returns the action to perform on a given column given the selected options
    * See:
    * https://dicom.nema.org/medical/dicom/current/output/html/part15.html#table_E.1-1
    * for the actions corresponding to DICOM keyword & option combination
    *
    * @param deidElem
    *   element from the E.1-1 table
    * @param options
    *   sequence of options to use in selecting the right action
    */
  def getAction(
      deidElem: DicomDeidElem,
      options: Seq[DeidOption] = Seq.empty
  ): DeidAction = {
    val deidActions = options
      .sortBy(_.priority)
      .map(_.getOptionAction(deidElem))
      .collect({ case Some(action) =>
        action
      })

    deidActions.headOption.getOrElse(deidElem.action) match {
      case "Z" | "Z/D"                              => Empty()
      case "D" | "D/X"                              => Dummify()
      case "C"                                      => Clean()
      case "U"                                      => Pseudonymize()
      case "X" | "X/Z" | "X/D" | "X/Z/D" | "X/Z/U*" => Drop()
      case "K"                                      => Keep()
    }
  }

  /** De-identifies a Dataframe that was loaded with the `dicomFile` format See:
    * https://dicom.nema.org/medical/dicom/current/output/html/part15.html#chapter_E
    *
    * @param dataframe
    *   columns need to be keywords as defined in the DICOM standard
    */
  def deidentify(
      dataframe: DataFrame,
      options: Seq[DeidOption] = Seq.empty
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
          getAction(deidElem, options).deidentify(keyword, vr)
        case (keyword, _, _) => Some(col(keyword))
      })
      .collect({ case Some(column) =>
        column
      })

    dataframe.select(columns: _*)
  }
}
