package ai.kaiko.dicom

import org.apache.spark.internal.Logging
import org.dcm4che3.data.ElementDictionary
import org.dcm4che3.imageio.plugins.dcm.DicomImageReaderSpi
import org.dcm4che3.io.DicomInputStream

import java.awt.image.BufferedImage
import java.io.File

/** Methods which help work with DICOM data. See also [[DicomFile]].
  */
object DicomHelper extends Logging {
  val standardDictionary = ElementDictionary.getStandardElementDictionary

  /** Read a DICOM image to a list of BufferedImage
    *
    * @param file
    *   a Java file
    * @return
    *   a list of buffered image (could be empty)
    */
  def readDicomImage(file: File): List[BufferedImage] = {
    val imgReaderSpi = new DicomImageReaderSpi
    val imgReader = imgReaderSpi.createReaderInstance(null)

    val dicomIn = new DicomInputStream(file)
    imgReader.setInput(dicomIn)
    val imgReadParam = imgReader.getDefaultReadParam

    val numImg = imgReader.getNumImages(true)

    (for (i <- 0 until numImg) yield imgReader.read(i, null)).toList
  }
}
