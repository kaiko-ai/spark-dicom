package ai.kaiko.dicom

import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.dcm4che3.data
import org.dcm4che3.util.TagUtils
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File

import funspec._

object TestDicomFile {
  // file which contains PersonName tag and at least 1 image
  val SOME_DICOM_FILEPATH =
    "src/test/resources/Pancreatic-CT-CBCT-SEG/Pancreas-CT-CB_001/07-06-2012-NA-PANCREAS-59677/201.000000-PANCREAS DI iDose 3-97846/1-001.dcm"
  lazy val SOME_DICOM_FILE = {
    val file = new File(TestDicomFile.SOME_DICOM_FILEPATH)
    assert(file.exists)
    file
  }

  val SOME_PATIENT_NAME = "Pancreas-CT-CB_001"
}

class TestDicomFile extends AnyFunSpec {
  val logger = LogManager.getLogger(classOf[TestDicomFile].getName);
  logger.setLevel(Level.DEBUG)

  describe("DicomFile") {
    val dicomFile = DicomFile.readDicomFile(TestDicomFile.SOME_DICOM_FILE)

    it("should be loaded") {
      assert(dicomFile.attrs != null)
    }

    describe("tags") {
      it("shouldn't be empty") { assert(!dicomFile.tags.isEmpty) }
      it("should contain PatientName tag") {
        assert(dicomFile.tags.contains(data.Tag.PatientName))
      }
    }

    describe("VRs") {
      it("shouldn't be empty") { assert(!dicomFile.vrs.isEmpty) }
      it("should contain the VR for PatientName") {
        assert(dicomFile.vrs.contains(data.VR.PN))
      }
    }
    describe("values") {
      it("shouldn't be empty") { assert(!dicomFile.values.isEmpty) }
      it("should contain the expected patient name") {
        assert(
          dicomFile.values.contains(
            StringValue(TestDicomFile.SOME_PATIENT_NAME)
          )
        )
      }
    }
  }
}
