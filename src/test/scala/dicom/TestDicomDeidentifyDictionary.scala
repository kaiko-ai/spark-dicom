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
package ai.kaiko.dicom

import org.scalatest.funspec.AnyFunSpec
import org.dcm4che3.data.Keyword.{valueOf => keywordOf}
import org.dcm4che3.data._

class TestDicomDeidentifyDictionary extends AnyFunSpec {

  val FIRST_ELEMENT_TAG = Tag.AccessionNumber

  describe("DicomDeidentifyDictionary") {
    it("builds list of dictionary for standard elements") {
      assert(DicomDeidentifyDictionary.elements.size > 0)
    }
    it("loads keyword") {
      val fstElem = DicomDeidentifyDictionary.elements(0)
      assert(fstElem.keyword === keywordOf(FIRST_ELEMENT_TAG))
    }
    it("loads action") {
      val fstElem = DicomDeidentifyDictionary.elements(0)
      assert(fstElem.action === ActionCode.Z)
    }
    it("loads name") {
      val fstElem = DicomDeidentifyDictionary.elements(0)
      assert(fstElem.name === "Accession Number")
    }
    it("loads tag") {
      val fstElem = DicomDeidentifyDictionary.elements(0)
      assert(fstElem.tag === FIRST_ELEMENT_TAG)
    }
  }
}
