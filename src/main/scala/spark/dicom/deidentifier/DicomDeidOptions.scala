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
package ai.kaiko.spark.dicom.deidentifier.options

import ai.kaiko.dicom.DicomDeidElem

sealed trait DeidOption {
  val priority: Int
  def getOptionAction(deid: DicomDeidElem): Option[String]
}
sealed case class RetainUids() extends DeidOption {
  val priority: Int = 9
  def getOptionAction(deid: DicomDeidElem): Option[String] =
    deid.retainUidsAction
}
sealed case class RetainDevId() extends DeidOption {
  val priority: Int = 8
  def getOptionAction(deid: DicomDeidElem): Option[String] =
    deid.retainDevIdAction
}
sealed case class RetainInstId() extends DeidOption {
  val priority: Int = 7
  def getOptionAction(deid: DicomDeidElem): Option[String] =
    deid.retainInstIdAction
}
sealed case class RetainPatChars() extends DeidOption {
  val priority: Int = 6
  def getOptionAction(deid: DicomDeidElem): Option[String] =
    deid.retainPatCharsAction
}
sealed case class RetainLongFullDates() extends DeidOption {
  val priority: Int = 5
  def getOptionAction(deid: DicomDeidElem): Option[String] =
    deid.retainLongFullDatesAction
}
sealed case class RetainLongModifDates() extends DeidOption {
  val priority: Int = 4
  def getOptionAction(deid: DicomDeidElem): Option[String] =
    deid.retainLongModifDatesAction
}
sealed case class CleanDesc() extends DeidOption {
  val priority: Int = 3
  def getOptionAction(deid: DicomDeidElem): Option[String] =
    deid.cleanDescAction
}
sealed case class CleanStrucCont() extends DeidOption {
  val priority: Int = 2
  def getOptionAction(deid: DicomDeidElem): Option[String] =
    deid.cleanStructContAction
}
sealed case class CleanGraph() extends DeidOption {
  val priority: Int = 1
  def getOptionAction(deid: DicomDeidElem): Option[String] =
    deid.cleanGraphAction
}
