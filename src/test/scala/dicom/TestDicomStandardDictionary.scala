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

import org.dcm4che3.data.VR
import org.scalatest.funspec.AnyFunSpec

class TestDicomStandardDictionary extends AnyFunSpec {
  describe("DicomStandardDictionary") {
    it("builds list of dictionary for standard elements") {
      assert(DicomStandardDictionary.elements.size > 0)
    }
    it("loads tag to int") {
      val fstElem = DicomStandardDictionary.elements(0)
      assert(fstElem.tag === 0x00080001)
    }
    it("loads VR to dcm4che's VR") {
      val fstElem = DicomStandardDictionary.elements(0)
      assert(fstElem.vr.isRight && fstElem.vr.right.get === VR.UL)
    }
  }
}
