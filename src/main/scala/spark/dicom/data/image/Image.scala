package ai.kaiko
package spark.dicom.data.image

import org.apache.spark.sql.functions._
import org.dcm4che3.imageio.plugins.dcm.{DicomImageReader, DicomImageReaderSpi}
import org.dcm4che3.io.DicomInputStream

import java.io.{ByteArrayInputStream, _}
import javax.imageio._

case class Image(data: Array[Byte], height: Option[Int] = Option.empty, width: Option[Int] = Option.empty)

object Image {

  def image(binary: Array[Byte], outputImageFormat: String = "png", dimensions: Boolean = false): Array[Image] = {
    val reader = new DicomImageReader(new DicomImageReaderSpi)
    scala.util.Try.apply({
      val dicomInputStream = new DicomInputStream(new ByteArrayInputStream(binary))
      reader.setInput(dicomInputStream)
      Seq.range(0, reader.getNumImages(true)).map{
        index => {
          val image = reader.read(index, reader.getDefaultReadParam)
          val imageByteStream = new ByteArrayOutputStream
          ImageIO.write(image, outputImageFormat, imageByteStream)
          val ba = imageByteStream.toByteArray
          if (dimensions) {
            val img = ImageIO.read(new ByteArrayInputStream(ba))
            Image(ba, Some(img.getHeight), Some(img.getWidth))
          } else {
            Image(ba)
          }
        }
      }
    }).getOrElse(Seq.empty).toArray
  }

  def toImage(outputImageFormat: String = "png") = udf {
    (b: Array[Byte]) => image(b, outputImageFormat)
  }
}
