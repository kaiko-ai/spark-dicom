package ai.kaiko.dicom

import org.scalatest.flatspec.AnyFlatSpec
import java.io.File

class TestDicomHelper extends AnyFlatSpec {
  lazy val SOME_DICOM_FILE = {
    val file = new File(
      "src/test/resources/Pancreatic-CT-CBCT-SEG/Pancreas-CT-CB_001/07-06-2012-NA-PANCREAS-59677/201.000000-PANCREAS DI iDose 3-97846/1-001.dcm"
    )
    assert(file.exists)
    file
  }

  "Dicom image" should "be read" in {
    val img = DicomHelper.readDicomImage(SOME_DICOM_FILE)
    assert(!img.isEmpty)
  }
}
