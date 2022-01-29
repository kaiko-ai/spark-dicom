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
import org.apache.spark.sql.Column
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
sealed case class Dummify() extends DeidAction {
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
