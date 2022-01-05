package ai.kaiko.spark.dicom

import ai.kaiko.dicom.DicomStandardDictionary
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.dcm4che3.data
import org.dcm4che3.io.DicomInputStream

import java.net.URI

object DicomFileReader extends Logging {
  val FIELD_NAME_PATH = "path"
  val METADATA_FIELDS = Array(
    StructField(FIELD_NAME_PATH, StringType, false)
  )

  def readDicomFile(
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      broadcastedHadoopConf: Broadcast[SerializableConfiguration],
      file: PartitionedFile
  ): Iterator[InternalRow] = {
    val path = new Path(new URI(file.filePath))
    val fs = path.getFileSystem(broadcastedHadoopConf.value.value)
    val status = fs.getFileStatus(path)

    val readPixelData = requiredSchema.fieldNames.contains(
      data.Keyword.valueOf(data.Tag.PixelData)
    )

    val fileStream = fs.open(status.getPath)
    val dicomInputStream = new DicomInputStream(fileStream)
    val attrs =
      if (readPixelData) dicomInputStream.readDataset
      else dicomInputStream.readDatasetUntilPixelData

    // TODO filters
    val mutableRow = new GenericInternalRow(requiredSchema.size)

    requiredSchema.fieldNames.zipWithIndex.foreach {
      // meta fields
      case (FIELD_NAME_PATH, i) => {
        val writer = InternalRow.getWriter(i, StringType)
        writer(mutableRow, UTF8String.fromString(status.getPath.toString))
      }
      // any other requested field should be a DICOM keyword
      case (keyword, i) => {
        DicomStandardDictionary.keywordMap.get(keyword) match {
          case None =>
            throw QueryExecutionErrors.unsupportedFieldNameError(keyword)
          case Some(stdElem) => {
            val sparkMapper = stdElem.vr.toOption.map {
              DicomSparkMapper.from
            } getOrElse DicomSparkMapper.DEFAULT_MAPPER
            val writer = InternalRow.getWriter(i, sparkMapper.sparkDataType)
            val value = sparkMapper.reader(attrs, stdElem.tag)
            if (value == null) {
              logError(f"$keyword has value null")
            }
            writer(mutableRow, value)
          }
        }
      }
    }
    Iterator.single(mutableRow)
  }
}
