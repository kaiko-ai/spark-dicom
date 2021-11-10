package ai.kaiko.dicom

import org.dcm4che3.imageio.plugins.dcm.DicomImageReaderSpi
import org.dcm4che3.io.DicomInputStream

import java.awt.image.BufferedImage
import java.io.File

object DicomHelper {
  def readDicomImage(file: File): BufferedImage = {
    val imgReaderSpi = new DicomImageReaderSpi
    val imgReader = imgReaderSpi.createReaderInstance(null)

    val dicomIn = new DicomInputStream(file)
    imgReader.setInput(dicomIn)
    val numImg = imgReader.getNumImages(true)
    val imgReadParam = imgReader.getDefaultReadParam
    imgReader.read(0, null)
  }
}
