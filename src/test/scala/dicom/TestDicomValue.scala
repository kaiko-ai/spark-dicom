package ai.kaiko.dicom

import org.dcm4che3.data.Attributes
import org.dcm4che3.data.Tag
import org.scalatest.flatspec.AnyFlatSpec

import java.time.LocalDate
import java.time.ZoneId
import java.util.Date

class TestDicomValue extends AnyFlatSpec {
  "VR DA" should "be parsed" in {
    val someTag = Tag.StudyDateAndTime
    val someLocalDateTime = LocalDate.of(1996, 7, 2).atStartOfDay

    val attrs = new Attributes(2)
    attrs.setDate(
      someTag,
      Date.from(someLocalDateTime.atZone(ZoneId.systemDefault).toInstant)
    )

    val parsedDate = DicomValue.readDicomValue(attrs, Tag.StudyDate)
    parsedDate match {
      case DateValue(value) =>
        assert(value === someLocalDateTime.toLocalDate)
      case v => throw new Exception("Not a DateValue: " ++ v.toString)
    }
  }

  "VR TM" should "be parsed" in {
    val someTag = Tag.StudyDateAndTime
    val someLocalDateTime = LocalDate.of(1996, 7, 2).atStartOfDay

    val attrs = new Attributes(2)
    attrs.setDate(
      someTag,
      Date.from(someLocalDateTime.atZone(ZoneId.systemDefault).toInstant)
    )

    val parsedTime = DicomValue.readDicomValue(attrs, Tag.StudyTime)
    parsedTime match {
      case TimeValue(value) => assert(value === someLocalDateTime.toLocalTime)
      case v => throw new Exception("Not a TimeValue: " ++ v.toString)
    }
  }
}
