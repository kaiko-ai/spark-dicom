package ai.kaiko.spark.dicom

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.csv.UnivocityGenerator
import org.apache.spark.sql.execution.datasources.CodecStreams
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType
import org.dcm4che3.data.UID
import org.dcm4che3.io.DicomInputStream
import org.dcm4che3.io.DicomOutputStream

import java.io.ByteArrayOutputStream
import java.nio.charset.Charset

class DicomOutputWriter(
    val path: String,
    dataSchema: StructType,
    context: TaskAttemptContext
) extends OutputWriter
    with Logging {

  val writer = {
    new DicomOutputStream(
      CodecStreams.createOutputStream(context, new Path(path)),
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
    val content = row.getBinary(contentColIndex)
    writer.write(content)
  }

  override def close(): Unit = writer.close()
}
