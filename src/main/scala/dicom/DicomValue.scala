package ai.kaiko.dicom

import org.dcm4che3.data.Attributes
import org.dcm4che3.data.VR
import org.apache.hadoop.shaded.org.checkerframework.common.value.qual.StringVal

// A sum type for read/parsed Dicom values
sealed trait DicomValue[+A] { val value: A }
case class StringValue(value: String) extends DicomValue[String]
case class UnsupportedValue() extends DicomValue[Unit] {
  lazy val value = {
    throw new Exception("Unsupported value")
  }
}

object DicomValue {
  def readDicomValue(attrs: Attributes, tag: Int): DicomValue[_] = {
    attrs.getVR(tag) match {
      case VR.AE => UnsupportedValue()
      case VR.AS => UnsupportedValue()
      case VR.AT => UnsupportedValue()
      case VR.CS => UnsupportedValue()
      case VR.DA => UnsupportedValue()
      case VR.DS => UnsupportedValue()
      case VR.DT => UnsupportedValue()
      case VR.FD => UnsupportedValue()
      case VR.FL => UnsupportedValue()
      case VR.IS => UnsupportedValue()
      case VR.LO => UnsupportedValue()
      case VR.LT => UnsupportedValue()
      case VR.OB => UnsupportedValue()
      case VR.OD => UnsupportedValue()
      case VR.OF => UnsupportedValue()
      case VR.OL => UnsupportedValue()
      case VR.OV => UnsupportedValue()
      case VR.OW => UnsupportedValue()
      // Person Name
      case VR.PN => StringValue(attrs.getString(tag))
      case VR.SH => UnsupportedValue()
      case VR.SL => UnsupportedValue()
      case VR.SQ => UnsupportedValue()
      case VR.SS => UnsupportedValue()
      case VR.ST => UnsupportedValue()
      case VR.SV => UnsupportedValue()
      case VR.TM => UnsupportedValue()
      case VR.UC => UnsupportedValue()
      case VR.UI => UnsupportedValue()
      case VR.UL => UnsupportedValue()
      case VR.UN => UnsupportedValue()
      case VR.UR => UnsupportedValue()
      case VR.US => UnsupportedValue()
      // Unlimited Text
      case VR.UT => StringValue(attrs.getString(tag))
      case VR.UV => UnsupportedValue()
    }
  }
}
