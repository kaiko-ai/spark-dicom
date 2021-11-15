package ai.kaiko.dicom

import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.dcm4che3.data
import org.dcm4che3.util.TagUtils
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File

import funspec._

class TestDicomFile extends AnyFlatSpec {

  val logger = LogManager.getLogger(classOf[TestDicomFile].getName);
  logger.setLevel(Level.DEBUG)

  val SOME_DICOM_FILEPATH =
    "src/test/resources/Pancreatic-CT-CBCT-SEG/Pancreas-CT-CB_001/07-06-2012-NA-PANCREAS-59677/201.000000-PANCREAS DI iDose 3-97846/1-001.dcm"

  "Dicom tags" should "be read" in {
    val file = new File(SOME_DICOM_FILEPATH)
    assert(file.exists)
    val metadata = DicomHelper.readDicomTagsAsMap(file)
    assert(metadata.contains("PatientName"))
    assert(
      metadata
        .get(
          data.Keyword.valueOf(data.Tag.PatientName)
        )
        .get
        .get === "Pancreas-CT-CB_001"
    )
  }

  "Dicom image" should "be read" in {
    val file = new File(SOME_DICOM_FILEPATH)
    assert(file.exists)
    val img = DicomHelper.readDicomImage(file)
    assert(img.isDefined)
  }
}
