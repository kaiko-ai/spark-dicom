package ai.kaiko.spark.dicom

import com.google.common.io.Closeables
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.dcm4che3.io.DicomInputStream

import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.net.URI

object DicomFileReader {

  def readDicomFile(
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      broadcastedHadoopConf: Broadcast[SerializableConfiguration],
      file: PartitionedFile
  ): Iterator[UnsafeRow] = {
    val path = new Path(new URI(file.filePath))
    val fs = path.getFileSystem(broadcastedHadoopConf.value.value)
    val status = fs.getFileStatus(path)

    val fileStream = fs.open(status.getPath)
    val dicomInputStream = new DicomInputStream(fileStream)
    val attrs = dicomInputStream.readDataset

    // TODO filters

    val writer = new UnsafeRowWriter(requiredSchema.length)
    val writeString = (i: Int, s: String) =>
      writer.write(i, UTF8String.fromString(s))

    writer.resetRowWriter()
    requiredSchema.fieldNames.zipWithIndex.foreach {
      // useful for debugging and testing
      case (DicomFileFormat.PATH, i) =>
        writeString(i, status.getPath.toString)
      // write Dicom Attributes as binary
      case (DicomFileFormat.CONTENT, i) => {
        val fileStream = fs.open(status.getPath)
        val dicomInputStream = new DicomInputStream(fileStream)
        val attrs = dicomInputStream.readDataset
        try {
          val bos = new ByteArrayOutputStream
          val out = new ObjectOutputStream(bos)
          out.writeObject(attrs)
          out.flush()

          writer.write(i, bos.toByteArray)
        } finally {
          Closeables.close(fileStream, true)
          Closeables.close(dicomInputStream, true)
        }
      }
      // any other requested field should be a DICOM keyword
      case (fieldName, _) =>
        throw QueryExecutionErrors.unsupportedFieldNameError(fieldName)
    }
    Iterator.single(writer.getRow)
  }
}
