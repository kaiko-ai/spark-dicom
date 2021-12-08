package ai.kaiko.dicom.data

import org.dcm4che3.data.Attributes
import org.dcm4che3.data.VR

/** A proxy case class, with data equivalent to
  * [[org.dcm4che3.data.Attributes]], which is compatible for
  * serialization/deserialization.
  */
case class ProxyAttributes(dataElements: Seq[DicomDataElement])
    extends Serializable

case class DicomDataElement(tag: Int, vr: VR, value: Object)
    extends Serializable

object ProxyAttributes {
  def from(attrs: Attributes): ProxyAttributes = {
    ProxyAttributes(
      attrs.tags.map(tag =>
        DicomDataElement(tag, attrs.getVR(tag), attrs.getValue(tag))
      )
    )
  }

  def to(proxyAttrs: ProxyAttributes): Attributes = {
    val attrs = new Attributes()
    proxyAttrs.dataElements.foreach {
      case DicomDataElement(tag, vr, value) => {
        attrs.setValue(tag, vr, value)
      }
    }
    attrs
  }
}
