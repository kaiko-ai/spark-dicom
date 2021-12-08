package ai.kaiko.dicom.data

import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpec
import org.dcm4che3.data.Attributes
import org.dcm4che3.data.Tag
import org.dcm4che3.data.VR
import java.io.File
import org.dcm4che3.io.DicomInputStream

class TestProxyAttributes extends AnyFunSpec with Matchers {
  describe("ProxyAttributes") {
    describe("on dummy data") {
      it("parses a dcm4che Attributes") {
        val someAttrs = new Attributes(1)
        someAttrs.setValue(Tag.PatientName, VR.PN, "Test")

        val proxyAttrs = ProxyAttributes.from(someAttrs)
        proxyAttrs.getClass shouldBe classOf[ProxyAttributes]
        proxyAttrs.dataElements.length shouldBe 1
        val dataElement = proxyAttrs.dataElements(0)
        dataElement.tag shouldBe Tag.PatientName
        dataElement.vr shouldBe VR.PN
        dataElement.value shouldBe "Test"
      }
      it("turns to a dcm4che Attributes") {
        val someProxyAttrs =
          new ProxyAttributes(
            Seq(DicomDataElement(Tag.PatientName, VR.PN, "Test"))
          )

        val attrs = ProxyAttributes.to(someProxyAttrs)
        attrs.getClass shouldBe classOf[Attributes]
        attrs.size shouldBe 1
        val tag = attrs.tags()(0)
        tag shouldBe Tag.PatientName
        attrs.getVR(tag) shouldBe VR.PN
        attrs.getValue(tag) shouldBe "Test"
      }
    }
    describe("on parsed file") {
      val someDicomFilepath =
        "src/test/resources/Pancreatic-CT-CBCT-SEG/Pancreas-CT-CB_001/07-06-2012-NA-PANCREAS-59677/201.000000-PANCREAS DI iDose 3-97846/1-001.dcm"
      val someDicomFile = {
        val file = new File(someDicomFilepath)
        assert(file.exists)
        file
      }
      val someAttrs = {
        val dicomInputStream = new DicomInputStream(someDicomFile)
        dicomInputStream.readDataset
      }

      it("keeps data") {
        val someProxyAttrs = ProxyAttributes.from(someAttrs)
        someProxyAttrs.getClass shouldBe classOf[ProxyAttributes]
        someProxyAttrs.dataElements.length shouldBe 76

        val attrs: Attributes = ProxyAttributes.to(someProxyAttrs)
        attrs shouldEqual someAttrs
      }
    }
  }
}
