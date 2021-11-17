package ai.kaiko.dicom

import org.dcm4che3.data.Keyword

object DicomTagKeyword {
  def tag: Int => String = Keyword.valueOf
}
