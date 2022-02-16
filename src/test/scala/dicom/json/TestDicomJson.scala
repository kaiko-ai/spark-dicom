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
package ai.kaiko.dicom.json

import org.dcm4che3.data.{VR, Attributes, Tag}
import org.scalatest.funspec.AnyFunSpec

class TestDicomJson extends AnyFunSpec {
  describe("TestDicomJson") {
    describe("attrs2jsonobject") {
      it("converts DICOM to JsonObject") {
        val someAttrs = {
          val attrs = new Attributes
          attrs.setString(0x300b1001, VR.UT, "test")
          attrs
        }
        val jsonObj = DicomJson.attrs2jsonobject(someAttrs)
        assert(jsonObj.getJsonObject("300B1001").getString("vr") === "UT")
        assert(
          jsonObj
            .getJsonObject("300B1001")
            .getJsonArray("Value")
            .getString(0) === "test"
        )
      }
      it("can be used to convert to String") {
        val someAttrs = {
          val attrs = new Attributes
          attrs.setString(0x300b1001, VR.UT, "test")
          attrs
        }
        val jsonObj = DicomJson.attrs2jsonobject(someAttrs)
        // write out to JSON
        val jsonStr = DicomJson.json2string(jsonObj)
        assert(
          jsonStr === "{\"300B1001\":{\"vr\":\"UT\",\"Value\":[\"test\"]}}"
        )
      }
      it("can be used to convert to String when Attributes is empty") {
        val someAttrs = new Attributes
        val jsonObj = DicomJson.attrs2jsonobject(someAttrs)
        // write out to JSON
        val jsonStr: String = DicomJson.json2string(jsonObj)
        assert(jsonStr === "{}")
      }
    }
    describe("seq2jsonarray") {
      it("converts DICOM Sequence to JsonArray") {
        val someSeq = {
          val attrs = new Attributes
          val seq = attrs.newSequence(Tag.DeidentificationMethodCodeSequence, 2)
          val nestedAttr1 = new Attributes
          val nestedAttr2 = new Attributes

          nestedAttr1.setString(0x300b1001, VR.UT, "test1")
          nestedAttr2.setString(0x300b1001, VR.UT, "test2")
          seq.add(nestedAttr1)
          seq.add(nestedAttr2)
          seq
        }
        val jsonArr = DicomJson.seq2jsonarray(someSeq)
        assert(
          jsonArr
            .getJsonObject(0)
            .getJsonObject("300B1001")
            .getString("vr") === "UT"
        )
        assert(
          jsonArr
            .getJsonObject(0)
            .getJsonObject("300B1001")
            .getJsonArray("Value")
            .getString(0) === "test1"
        )
      }
      it("can be used to convert to String") {
        val someSeq = {
          val attrs = new Attributes
          val seq = attrs.newSequence(Tag.DeidentificationMethodCodeSequence, 2)
          val nestedAttr1 = new Attributes
          val nestedAttr2 = new Attributes

          nestedAttr1.setString(0x300b1001, VR.UT, "test1")
          nestedAttr2.setString(0x300b1001, VR.UT, "test2")
          seq.add(nestedAttr1)
          seq.add(nestedAttr2)
          seq
        }
        val jsonArr = DicomJson.seq2jsonarray(someSeq)
        val jsonStr = DicomJson.json2string(jsonArr)
        assert(
          jsonStr === "[{\"300B1001\":{\"vr\":\"UT\",\"Value\":[\"test1\"]}},{\"300B1001\":{\"vr\":\"UT\",\"Value\":[\"test2\"]}}]"
        )
      }
      it("can be used to convert to String when Attributes is empty") {
        val someSeq = {
          val attrs = new Attributes
          val seq = attrs.newSequence(Tag.DeidentificationMethodCodeSequence, 0)
          seq
        }
        val jsonArr = DicomJson.seq2jsonarray(someSeq)
        val jsonStr = DicomJson.json2string(jsonArr)
        assert(jsonStr === "[]")
      }
    }
  }
}
