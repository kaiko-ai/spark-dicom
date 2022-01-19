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
package ai.kaiko.spark.dicom

import ai.kaiko.dicom.DicomStandardDictionary
import ai.kaiko.dicom.ScalaVR
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.unsafe.types.UTF8String
import org.dcm4che3.data.Attributes
import org.dcm4che3.data.VR
import org.scalatest.funspec.AnyFunSpec

import java.time.LocalDate
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util.logging.Logger

object TestDicomSparkMapper {
  def getSomeTag(vr: VR): Option[Int] = {
    DicomStandardDictionary.elements
      .find(stdElem => stdElem.vr.map(_.equals(vr)).toOption.getOrElse(false))
      .map(stdElem => stdElem.tag)
  }
  def getSomeValue(vr: VR): Option[Any] = {
    import VR._
    vr match {
      case AE | AS | CS | DS | DT | IS | LO | LT | SH | ST | UC | UI | UR |
          UT =>
        Some(f"someValueFor$vr")
      case AT                => Some("1")
      case PN                => Some("somePersonName")
      case FL | FD           => Some(395.toDouble)
      case SL | SS | US | UL => Some(492.toInt)
      case SV | UV           => Some(93.toLong)
      case DA                => Some(LocalDate.of(2022, 1, 1))
      case TM                => Some(LocalTime.of(12, 0))
      case _                 => None
    }
  }
  def getExpectedValue(vr: VR, value: Option[Any]): Option[Any] = {
    import VR._
    vr match {
      case AE | AS | CS | DS | DT | IS | LO | LT | SH | ST | UC | UI | UR |
          UT =>
        value.map(v => UTF8String.fromString(v.asInstanceOf[String]))
      case AT =>
        value.map(v =>
          UTF8String.fromString(
            v.asInstanceOf[String].reverse.padTo(8, '0').reverse
          )
        )
      case PN =>
        value.map(v =>
          InternalRow.fromSeq(
            Seq(v.asInstanceOf[String], "", "").map(UTF8String.fromString)
          )
        )
      case FL | FD =>
        value.map(v => ArrayData.toArrayData(Array(v.asInstanceOf[Double])))
      case SL | SS | US | UL =>
        value.map(v => ArrayData.toArrayData(Array(v.asInstanceOf[Int])))
      case SV | UV =>
        value.map(v => ArrayData.toArrayData(Array(v.asInstanceOf[Long])))
      case DA =>
        value.map(v =>
          UTF8String.fromString(
            v.asInstanceOf[LocalDate]
              .format(DateTimeFormatter.ISO_LOCAL_DATE)
          )
        )
      case TM =>
        value.map(v =>
          UTF8String.fromString(
            v.asInstanceOf[LocalTime]
              .format(DateTimeFormatter.ISO_LOCAL_TIME)
          )
        )
      case _ => None
    }
  }

  val SOME_ATTRS = {
    val attrs = new Attributes
    val mbZip = VR.values.map(vr => {
      val mbTag = getSomeTag(vr)
      val mbSetter = ScalaVR.getSetter(vr)
      val mbValue = getSomeValue(vr)
      (
        vr,
        (mbTag zip mbSetter zip mbValue).headOption.map { case ((v1, v2), v3) =>
          (v1, v2, v3)
        }
      )
    })
    val missingVrs = mbZip.collect { case (vr, mbTpl) if mbTpl.isEmpty => vr }
    Logger.getGlobal.warning(
      f"Value not set in attrs for VRs: ${missingVrs.mkString(", ")}"
    )
    mbZip foreach {
      case (_, Some((tag, setter, value))) => setter(attrs, tag, value)
      case (_, None)                       =>
    }
    attrs
  }
}

class TestDicomSparkMapper extends AnyFunSpec {
  import TestDicomSparkMapper._

  describe("DicomSparkMapper") {
    VR.values.foreach { vr =>
      val mapper = DicomSparkMapper.from(vr)
      val mbTag = getSomeTag(vr)
      val mbExpectedValue = getExpectedValue(vr, getSomeValue(vr))
      (mbTag zip mbExpectedValue).headOption match {
        case Some((tag, expectedValue)) => {
          describe(f"for VR $vr") {
            it("reads") {
              assert { mapper.reader(SOME_ATTRS, tag) === expectedValue }
            }
            it("ingests to InternalRow") {
              val mutableRow = new GenericInternalRow(1)
              val value = mapper.reader(SOME_ATTRS, tag)
              val writer = InternalRow.getWriter(0, mapper.sparkDataType)
              writer(mutableRow, value)
              assert { mutableRow.get(0, mapper.sparkDataType) === value }
            }
            it("ingests to InternalRow even when value is null") {
              val mutableRow = new GenericInternalRow(1)
              val value = mapper.reader(new Attributes(), tag)
              val writer = InternalRow.getWriter(0, mapper.sparkDataType)
              writer(mutableRow, value)
              assert { mutableRow.get(0, mapper.sparkDataType) === value }
            }
          }
        }
        case None =>
          Logger.getGlobal.warning(
            f"Could not build tests 'TestDicomSparkMapper for VR $vr'"
          )
      }
    }
  }
}

class ExtraTestDicomSparkMapper extends AnyFunSpec {
  import TestDicomSparkMapper._

  describe("DicomSparkMapper") {
    it("reads TM with 6 digits for nanoseconds") {
      val someTag = getSomeTag(VR.TM).get
      val mapper = DicomSparkMapper.from(VR.TM)

      val someAttrs = {
        val attrs = new Attributes
        attrs.setString(someTag, VR.TM, "122734.625000")
        attrs
      }

      val expectedValue = UTF8String.fromString(
        LocalTime
          .of(12, 27, 34, 625000000)
          .format(DateTimeFormatter.ISO_LOCAL_TIME)
      )

      assert { mapper.reader(someAttrs, someTag) === expectedValue }
    }
  }
}
