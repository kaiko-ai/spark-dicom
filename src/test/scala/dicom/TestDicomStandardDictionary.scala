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
