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

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

sealed trait DeidAction {
  def makeDeidentifiedColumn(keyword: String): Option[Column]
}
sealed case class Empty(emptyValue: Option[Any]) extends DeidAction {
  def makeDeidentifiedColumn(keyword: String): Option[Column] =
    emptyValue.map(lit(_).as(keyword))
}
sealed case class Dummify(dummyValue: Option[Any]) extends DeidAction {
  def makeDeidentifiedColumn(keyword: String): Option[Column] =
    dummyValue.map(lit(_).as(keyword))
}
sealed case class Clean() extends DeidAction {
  def makeDeidentifiedColumn(keyword: String): Option[Column] = Some(
    lit("ToClean").as(keyword)
  )
}
sealed case class Pseudonymize() extends DeidAction {
  def makeDeidentifiedColumn(keyword: String): Option[Column] = Some(
    lit("ToPseudonymize").as(keyword)
  )
}
sealed case class Drop() extends DeidAction {
  def makeDeidentifiedColumn(keyword: String): Option[Column] = None
}
sealed case class Keep() extends DeidAction {
  def makeDeidentifiedColumn(keyword: String): Option[Column] = Some(
    col(keyword)
  )
}
