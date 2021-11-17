package ai.kaiko.dicom

import org.scalatest.flatspec.AnyFlatSpec

class TestDicomHelper extends AnyFlatSpec {
  "Dicom image" should "be read" in {
    val img = DicomHelper.readDicomImage(TestDicomFile.SOME_DICOM_FILE)
    assert(!img.isEmpty)
  }
}
