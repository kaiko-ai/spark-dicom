package ai.kaiko.dicom

import org.dcm4che3.data._

import java.time.LocalDate
import java.time.LocalTime
import java.time.format.DateTimeFormatter

object ScalaVR {

  /** See also [[VR.toValue]] */
  def getSetter(vr: VR): Option[(Attributes, Int, Any) => Unit] = {
    import VR._
    vr match {
      case AE | AS | AT | CS | DS | DT | IS | LO | LT | SH | ST | UC | UI | UR |
          UT =>
        Some((attrs, tag, value) =>
          attrs.setString(tag, vr, value.asInstanceOf[String])
        )
      case PN =>
        Some((attrs, tag, value) =>
          attrs.setString(tag, vr, value.asInstanceOf[String])
        )
      case FL | FD =>
        Some((attrs, tag, value) =>
          attrs.setDouble(tag, vr, value.asInstanceOf[Double])
        )
      case SL | SS | US | UL =>
        Some((attrs, tag, value) =>
          attrs.setInt(tag, vr, value.asInstanceOf[Int])
        )
      case SV | UV =>
        Some((attrs, tag, value) =>
          attrs.setLong(tag, vr, value.asInstanceOf[Long])
        )
      case DA =>
        Some((attrs, tag, value) =>
          attrs.setString(
            tag,
            vr,
            value
              .asInstanceOf[LocalDate]
              .format(
                DateTimeFormatter
                  .ofPattern("yyyyMMdd")
              )
          )
        )
      case TM =>
        Some((attrs, tag, value) =>
          attrs.setString(
            tag,
            vr,
            value
              .asInstanceOf[LocalTime]
              .format(
                DateTimeFormatter
                  .ofPattern("HHmmss[.SSS][.SS][.S]")
              )
          )
        )
      case _ => None
    }
  }
}
