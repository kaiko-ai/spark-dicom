package ai.kaiko.dicom

import org.dcm4che3.data.Attributes
import org.dcm4che3.data.Keyword
import org.dcm4che3.data.VR
import org.dcm4che3.io.DicomInputStream

import java.io.File

/** A wrapper class around DICOM attributes.
  *
  * Typically, one would want to do
  *
  * ```scala
  * val dis = new DicomInputStream(file)
  * val attrs = dis.readDataset
  * val data = DicomFile(attrs)
  * ```
  *
  * Use `DicomFile.readDicomFile` if you only need this data only, not image.
  *
  * @param attrs
  *   DICOM attributes
  */
case class DicomFile(attrs: Attributes) {
  // tag = id of entry
  val tags: Array[Int] = attrs.tags
  // keyword = human readable id of entry (from DICOM standard)
  val keywords: Array[String] = tags.map(Keyword.valueOf)
  // vr = type of entry (defined in DICOM standard)
  val vrs: Array[VR] = tags.map(attrs.getVR)
  // value = value of entry (for now, we read string only)
  val values: Array[DicomValue[_]] =
    tags.map(DicomValue.readDicomValue(attrs, _))

  lazy val tagsToKeyword: Map[Int, String] = (tags zip keywords).toMap
  lazy val tagsToVr: Map[Int, VR] = (tags zip vrs).toMap
  lazy val tagsToValue: Map[Int, DicomValue[_]] = (tags zip values).toMap
}

object DicomFile {

  /** Read a DicomFile from a Java [[File]]
    *
    * WARNING: use only if you do not need to get BufferedImage from pixel data.
    *
    * @param file
    * @return
    */
  def readDicomFile(file: File): DicomFile = {
    val dicomInputStream = new DicomInputStream(file)
    readDicomFile(dicomInputStream)
  }

  /** Read a DicomFile from a [[DicomInputStream]]
    *
    * WARNING: use only if you do not need to get BufferedImage from pixel data.
    *
    * @param dicomInputStream
    * @return
    */
  def readDicomFile(dicomInputStream: DicomInputStream): DicomFile = {
    val attrs = dicomInputStream.readDataset
    DicomFile(attrs)
  }
}
