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
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.dcm4che3.data._


case class DicomDeidAction(
  keyword: String,
  vr: VR,
  action: String
)

case class DicomDeidColumns(
  keep: Seq[Column],
  empty: Seq[Column],
  replace: Seq[Column],
  clean: Seq[Column],
  pseudonymize: Seq[Column],
  drop: Seq[String]
)

object DicomDeidentifier {

  // instead of being hardcoded, will later be calculated based on security profile
  val keepActions = Seq("K")
  val emptyActions = Seq("Z", "Z/D")
  val replaceActions = Seq("D", "D/X")
  val cleanActions = Seq("C")
  val pseudonymizeActions = Seq("U")
  val dropActions = Seq("X", "X/Z", "X/D", "X/Z/D", "X/Z/U*")

  lazy val deidActionGroups = DicomDeidentifyDictionary.elements
      .map(deidElem => (deidElem, DicomStandardDictionary.keywordMap.get(deidElem.keyword)))
      .collect({
        case (deidElem, Some(stdElem)) => (deidElem, stdElem.vr)
      })
      .collect({
        case (deidElem, Right(vr)) => DicomDeidAction(deidElem.keyword, vr, deidElem.action)
      })
      .groupBy(_.action)

  /** De-identifies a Dataframe that was loaded with the `dicomFile` format
   * according to the Basic Confidentiality Profile. See:
   *  https://dicom.nema.org/medical/dicom/current/output/html/part15.html#chapter_E
   *
   *  @param dataframe columns need to be keywords as defined in the DICOM standard
   */
  def deidentify(dataframe: DataFrame): DataFrame = {

    // Dataframe columns should be DICOM keywords
    val cols = DicomDeidColumns(
      keep =
        keepActions
          .map(deidActionGroups.getOrElse(_, Array.empty))
          .flatten
          .map(deid => col(deid.keyword)),
      empty = 
        emptyActions
          .map(deidActionGroups.getOrElse(_, Array.empty))
          .flatten
          .map(deid => (deid.keyword, DicomDeidentifyDictionary.getEmptyValue(deid.vr)))
          .collect({
            case (keyword, Some(emptyVal)) => lit(emptyVal).as(keyword)
          }),
      replace =
        replaceActions
          .map(deidActionGroups.getOrElse(_, Array.empty))
          .flatten
          .map(deid => (deid.keyword, DicomDeidentifyDictionary.getDummyValue(deid.vr)))
          .collect({
            case (keyword, Some(dummyVal)) => lit(dummyVal).as(keyword)
          }),
      clean = 
        cleanActions
          .map(deidActionGroups.getOrElse(_, Array.empty))
          .flatten
          .map(deid => lit("ToClean").as(deid.keyword)),
      pseudonymize =
        pseudonymizeActions
          .map(deidActionGroups.getOrElse(_, Array.empty))
          .flatten
          .map(deid => lit("ToPseudo").as(deid.keyword)),
      drop = 
        dropActions
          .map(deidActionGroups.getOrElse(_, Array.empty))
          .flatten
          .map(_.keyword)
    )

    dataframe
      .select(col("*") +: (
        cols.keep ++ 
        cols.empty ++ 
        cols.replace ++ 
        cols.clean ++ 
        cols.pseudonymize
      ): _*)
      .drop(cols.drop: _*)
  }
}
