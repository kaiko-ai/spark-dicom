package ai.kaiko.spark.dicom

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.CodecStreams
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType
import org.dcm4che3.data.Attributes
import org.dcm4che3.data.UID
import org.dcm4che3.io.DicomOutputStream

import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import org.dcm4che3.data.Tag

class DicomOutputWriter(
    val path: String,
    dataSchema: StructType,
    context: TaskAttemptContext
) extends OutputWriter
    with Logging {

  val writer = {
    new DicomOutputStream(
      CodecStreams.createOutputStream(context, new Path(path)),
      // note: ImplicitVRLittleEndian is the default transfer syntax in DICOM standard
      UID.ExplicitVRLittleEndian
    )
  }

  override def write(row: InternalRow): Unit = {
    val contentColIndex =
      dataSchema.fields.indexWhere(_.name == DicomFileFormat.CONTENT)
    if (contentColIndex == -1)
      throw new Exception(
        "Missing column '" + DicomFileFormat.CONTENT + "'"
      )

    // get Dicom Attributes by deserializing
    val attrsBytes = row.getBinary(contentColIndex)
    val bis = new ByteArrayInputStream(attrsBytes)
    val out = new ObjectInputStream(bis)
    val attrs = out.readObject.asInstanceOf[Attributes]

    val fileMetaInformationAttrs =
      attrs.getNestedDataset(Tag.FileMetaInformationGroupLength)

    // make attrs only contain dataset, not file meta info
    attrs.remove(Tag.FileMetaInformationGroupLength)

    // write to file
    writer.writeDataset(fileMetaInformationAttrs, attrs)
  }

  override def close(): Unit = writer.close()
}
