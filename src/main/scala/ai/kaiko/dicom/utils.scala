package ai.kaiko.dicom

import org.dcm4che3.data.Attributes
import org.dcm4che3.data.UID
import org.dcm4che3.io.DicomInputStream
import org.dcm4che3.io.DicomOutputStream

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

object DicomUtils {
  def attrs2bytearray(attrs: Attributes): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val dos =
      new DicomOutputStream(
        baos,
        UID.ExplicitVRLittleEndian
      )
    attrs.writeTo(dos)
    dos.flush
    baos.toByteArray
  }

  def bytearray2attrs(bytes: Array[Byte]): Attributes = {
    val bais = new ByteArrayInputStream(bytes)
    val dis = new DicomInputStream(bais)
    dis.readDataset
  }
}
