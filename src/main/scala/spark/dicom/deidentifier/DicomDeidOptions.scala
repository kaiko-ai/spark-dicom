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
import scala.reflect.runtime.{universe => ru}

sealed abstract class DeidOption(val priority: Int)
case object DeidOption {
  val mirror =
    ru.runtimeMirror(getClass.getClassLoader)

  // Note: Symbol class will be deprecated in scala 3 
  // http://dotty.epfl.ch/docs/reference/dropped-features/symlits.html
  lazy val values: List[DeidOption] = ru
    .typeOf[DeidOption]
    .typeSymbol
    .asClass
    .knownDirectSubclasses
    .map(symb => {
      val module = mirror.staticModule(symb.fullName)
      val obj = mirror.reflectModule(module)
      obj.instance.asInstanceOf[DeidOption]
    })
    .toList
    .sortBy(_.priority)
}

case object CleanGraph extends DeidOption(1)
case object CleanStructCont extends DeidOption(2)
case object CleanDesc extends DeidOption(3)
case object RetainLongModifDates extends DeidOption(4)
case object RetainLongFullDates extends DeidOption(5)
case object RetainPatChars extends DeidOption(6)
case object RetainInstId extends DeidOption(7)
case object RetainDevId extends DeidOption(8)
case object RetainUids extends DeidOption(9)
