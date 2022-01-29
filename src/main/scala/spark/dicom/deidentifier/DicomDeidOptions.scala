package ai.kaiko.spark.dicom.deidentifier.options

import ai.kaiko.dicom.DicomDeidElem

sealed trait DeidOption {
  val priority: Int
  def getOptionAction(deid: DicomDeidElem): Option[String]
}
sealed case class RetainUids() extends DeidOption {
  val priority: Int = 9
  def getOptionAction(deid: DicomDeidElem): Option[String] =
    deid.retainUidsAction
}
sealed case class RetainDevId() extends DeidOption {
  val priority: Int = 8
  def getOptionAction(deid: DicomDeidElem): Option[String] =
    deid.retainDevIdAction
}
sealed case class RetainInstId() extends DeidOption {
  val priority: Int = 7
  def getOptionAction(deid: DicomDeidElem): Option[String] =
    deid.retainInstIdAction
}
sealed case class RetainPatChars() extends DeidOption {
  val priority: Int = 6
  def getOptionAction(deid: DicomDeidElem): Option[String] =
    deid.retainPatCharsAction
}
sealed case class RetainLongFullDates() extends DeidOption {
  val priority: Int = 5
  def getOptionAction(deid: DicomDeidElem): Option[String] =
    deid.retainLongFullDatesAction
}
sealed case class RetainLongModifDates() extends DeidOption {
  val priority: Int = 4
  def getOptionAction(deid: DicomDeidElem): Option[String] =
    deid.retainLongModifDatesAction
}
sealed case class CleanDesc() extends DeidOption {
  val priority: Int = 3
  def getOptionAction(deid: DicomDeidElem): Option[String] =
    deid.cleanDescAction
}
sealed case class CleanStrucCont() extends DeidOption {
  val priority: Int = 2
  def getOptionAction(deid: DicomDeidElem): Option[String] =
    deid.cleanStructContAction
}
sealed case class CleanGraph() extends DeidOption {
  val priority: Int = 1
  def getOptionAction(deid: DicomDeidElem): Option[String] =
    deid.cleanGraphAction
}
