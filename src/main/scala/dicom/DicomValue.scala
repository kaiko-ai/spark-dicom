package ai.kaiko.dicom

import org.dcm4che3.data.Attributes
import org.dcm4che3.data.VR

import java.time.LocalDate
import java.time.LocalTime
import java.time.format.DateTimeFormatter

/** A sum type to represent parsed DICOM values. Can be one of StringValue,
  * DateValue, UnsupportedValue
  */
sealed trait DicomValue[+A] { val value: A }

case class IntValue(value: Int) extends DicomValue[Int]

case class StringValue(value: String) extends DicomValue[String]

case class DateValue(value: LocalDate) extends DicomValue[LocalDate]
object DateValue {
  val SPARK_FORMATTER = DateTimeFormatter.ISO_DATE
}

case class TimeValue(value: LocalTime) extends DicomValue[LocalTime]
object TimeValue {
  val SPARK_FORMATTER = DateTimeFormatter.ISO_TIME
}

case class UnsupportedValue(vr_name: String) extends DicomValue[Nothing] {
  lazy val value = {
    throw new Exception("No value for an unsupported VR: " ++ vr_name)
  }
}

object DicomValue {

  /** Read from DICOM [[Attributes]] for a specific [[Tag]] to a [[DicomValue]]
    *
    * @param attrs
    *   DICOM attributes
    * @param tag
    *   target tag
    * @return
    *   One of the case class of DicomValue: StringValue, UnsupportedValue
    */
  def readDicomValue(attrs: Attributes, tag: Int): DicomValue[_] = {
    val vr = attrs.getVR(tag)
    // https://dicom.nema.org/dicom/2013/output/chtml/part05/sect_6.2.html#table_6.2-1
    vr match {
      case VR.AE => StringValue(attrs.getString(tag).strip)
      case VR.AS => StringValue(attrs.getString(tag))
      case VR.AT => IntValue(attrs.getInt(tag, 0))
      case VR.CS => StringValue(attrs.getString(tag).strip)
      case VR.DA => {
        val dateStr = attrs.getString(tag)
        val parser = DateTimeFormatter.ofPattern("yyyyMMdd")
        val value = LocalDate.parse(dateStr, parser)
        DateValue(value)
      }
      case VR.DS => UnsupportedValue(vr.name())
      case VR.DT => UnsupportedValue(vr.name())
      case VR.FD => UnsupportedValue(vr.name())
      case VR.FL => UnsupportedValue(vr.name())
      case VR.IS => UnsupportedValue(vr.name())
      case VR.LO => UnsupportedValue(vr.name())
      case VR.LT => UnsupportedValue(vr.name())
      case VR.OB => UnsupportedValue(vr.name())
      case VR.OD => UnsupportedValue(vr.name())
      case VR.OF => UnsupportedValue(vr.name())
      case VR.OL => UnsupportedValue(vr.name())
      case VR.OV => UnsupportedValue(vr.name())
      case VR.OW => UnsupportedValue(vr.name())
      // Person Name
      case VR.PN => StringValue(attrs.getString(tag))
      case VR.SH => UnsupportedValue(vr.name())
      case VR.SL => UnsupportedValue(vr.name())
      case VR.SQ => UnsupportedValue(vr.name())
      case VR.SS => UnsupportedValue(vr.name())
      case VR.ST => UnsupportedValue(vr.name())
      case VR.SV => UnsupportedValue(vr.name())
      case VR.TM => {
        val timeStr = attrs.getString(tag)
        val parser = DateTimeFormatter.ofPattern("HHmmss[.SSS][.SS][.S]")
        val value = LocalTime.parse(timeStr, parser)
        TimeValue(value)
      }
      case VR.UC => UnsupportedValue(vr.name())
      case VR.UI => UnsupportedValue(vr.name())
      case VR.UL => UnsupportedValue(vr.name())
      case VR.UN => UnsupportedValue(vr.name())
      case VR.UR => UnsupportedValue(vr.name())
      case VR.US => UnsupportedValue(vr.name())
      // Unlimited Text
      case VR.UT => StringValue(attrs.getString(tag))
      case VR.UV => UnsupportedValue(vr.name())
    }
  }
}
