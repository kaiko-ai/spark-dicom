package ai.kaiko.spark.dicom.deidentifier

import ai.kaiko.dicom.DicomDeidentifyDictionary
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.dcm4che3.data._

sealed trait DeidAction {
  def deidentify(keyword: String, vr: VR): Option[Column]
}
sealed case class Empty() extends DeidAction {
  def deidentify(keyword: String, vr: VR): Option[Column] =
    DicomDeidentifyDictionary.getEmptyValue(vr) match {
      case Some(emptyVal) => Some(lit(emptyVal).as(keyword))
      case _              => None
    }
}
sealed case class Dummify() extends DeidAction {
  def deidentify(keyword: String, vr: VR): Option[Column] =
    DicomDeidentifyDictionary.getDummyValue(vr) match {
      case Some(dummyVal) => Some(lit(dummyVal).as(keyword))
      case _              => None
    }
}
sealed case class Clean() extends DeidAction {
  def deidentify(keyword: String, vr: VR): Option[Column] = Some(
    lit("ToClean").as(keyword)
  )
}
sealed case class Pseudonymize() extends DeidAction {
  def deidentify(keyword: String, vr: VR): Option[Column] = Some(
    lit("ToPseudonymize").as(keyword)
  )
}
sealed case class Drop() extends DeidAction {
  def deidentify(keyword: String, vr: VR): Option[Column] = None
}
sealed case class Keep() extends DeidAction {
  def deidentify(keyword: String, vr: VR): Option[Column] = Some(col(keyword))
}
