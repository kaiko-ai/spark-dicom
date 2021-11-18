package ai.kaiko.dicom

import org.apache.hadoop.shaded.org.checkerframework.common.value.qual.StringVal
import org.dcm4che3.data.Attributes
import org.dcm4che3.data.VR

/** A sum type to represent parsed DICOM values. Can be one of StringValue,
  * UnsupportedValue
  */
sealed trait DicomValue[+A] { val value: A }
case class StringValue(value: String) extends DicomValue[String]
case class UnsupportedValue(vr_name: String) extends DicomValue[Nothing] {
  lazy val value = {
    throw new Exception("Unsupported value")
  }
}

object DicomValue {
  def readDicomValue(attrs: Attributes, tag: Int): DicomValue[_] = {
    val vr = attrs.getVR(tag)
    vr match {
      case VR.AE => UnsupportedValue(vr.name())
      case VR.AS => UnsupportedValue(vr.name())
      case VR.AT => UnsupportedValue(vr.name())
      // String of characters with leading or trailing spaces being non-significant
      case VR.CS => StringValue(attrs.getString(tag).strip)
      case VR.DA => UnsupportedValue(vr.name())
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
      case VR.TM => UnsupportedValue(vr.name())
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
