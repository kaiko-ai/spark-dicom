package ai.kaiko.dicom

import org.dcm4che3.data.Keyword
import org.dcm4che3.imageio.plugins.dcm.DicomImageReaderSpi
import org.dcm4che3.io.DicomInputStream
import org.dcm4che3.util.TagUtils

import java.awt.image.BufferedImage
import java.io.File
import org.dcm4che3.data.VR

object DicomHelper {
  def readDicomTagsAsMap(
      file: File
  ): Map[String, Option[String]] = {
    val dicomIn = new DicomInputStream(file)
    val attrs = dicomIn.readDataset
    attrs.tags
      .map(tag =>
        (
          Keyword.valueOf(tag),
          attrs.getVR(tag) match {
            case VR.AE => None
            case VR.AS => None
            case VR.AT => None
            case VR.CS => None
            case VR.DA => None
            case VR.DS => None
            case VR.DT => None
            case VR.FD => None
            case VR.FL => None
            case VR.IS => None
            case VR.LO => None
            case VR.LT => None
            case VR.OB => None
            case VR.OD => None
            case VR.OF => None
            case VR.OL => None
            case VR.OV => None
            case VR.OW => None
            // Person Name
            case VR.PN => Some(attrs.getString(tag))
            case VR.SH => None
            case VR.SL => None
            case VR.SQ => None
            case VR.SS => None
            case VR.ST => None
            case VR.SV => None
            case VR.TM => None
            case VR.UC => None
            case VR.UI => None
            case VR.UL => None
            case VR.UN => None
            case VR.UR => None
            case VR.US => None
            // Unlimited Text
            case VR.UT => Some(attrs.getString(tag))
            case VR.UV => None
          }
        )
      )
      .toMap
  }

  def readDicomImage(file: File): Option[BufferedImage] = {
    val imgReaderSpi = new DicomImageReaderSpi
    val imgReader = imgReaderSpi.createReaderInstance(null)

    val dicomIn = new DicomInputStream(file)
    imgReader.setInput(dicomIn)
    val numImg = imgReader.getNumImages(true)
    if (numImg > 0) {
      val imgReadParam = imgReader.getDefaultReadParam
      Some(imgReader.read(0, null))
    } else {
      None
    }
  }
}
