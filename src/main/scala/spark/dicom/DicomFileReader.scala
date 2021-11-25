package ai.kaiko.spark.dicom

import ai.kaiko.dicom.DicomFile
import ai.kaiko.dicom.DicomHelper
import ai.kaiko.dicom.StringValue
import ai.kaiko.dicom.UnsupportedValue
import com.google.common.io.ByteStreams
import com.google.common.io.Closeables
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
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

import java.net.URI

object DicomFileReader {
  def inferSchema(
      conf: Configuration,
      file: FileStatus,
      includeDefault: Boolean
  ): StructType = {
    val fs = file.getPath.getFileSystem(conf)
    val fileStream = fs.open(file.getPath)
    val dicomInputStream = new DicomInputStream(fileStream)
    val dicomFile = DicomFile.readDicomFile(dicomInputStream)
    val fields = (dicomFile.keywords zip dicomFile.vrs)
      .map { case (kw, vr) =>
        DicomHelper.maybeBuildSparkStructFieldFrom(kw, vr)
      }
      .filter((mb) => mb.isDefined)
      .map((mb) => mb.get)
    // add file attributes to default schema
    if (!includeDefault) StructType(fields)
    else StructType(DicomFileFormat.DEFAULT_SCHEMA.union(StructType(fields)))
  }

  def inferSchema(
      conf: Configuration,
      files: Seq[FileStatus],
      includeDefault: Boolean
  ): StructType = {
    val structTypes: Seq[StructType] = files
      .map(file =>
        inferSchema(
          conf,
          file,
          includeDefault = false
        )
      )
    val structType: StructType =
      structTypes.fold(DicomFileFormat.DEFAULT_SCHEMA)((x, y) =>
        StructType(x.fields.union(y.fields))
      )
    structType
  }

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
    val dicomFile = DicomFile.readDicomFile(dicomInputStream)

    // TODO filters

    val writer = new UnsafeRowWriter(requiredSchema.length)
    writer.resetRowWriter()
    requiredSchema.fieldNames.zipWithIndex.foreach {
      case (DicomFileFormat.PATH, i) =>
        writer.write(i, UTF8String.fromString(status.getPath.toString))
      case (DicomFileFormat.CONTENT, i) =>
        val fileStream = fs.open(status.getPath)
        val dicomInputStream = new DicomInputStream(fileStream)
        try {
          writer.write(i, ByteStreams.toByteArray(dicomInputStream))
        } finally {
          Closeables.close(fileStream, true)
          Closeables.close(dicomInputStream, true)
        }
      case (keyword, i) => {
        val keywordIndex = dicomFile.keywords.indexOf(keyword)
        if (keywordIndex == -1) {
          throw QueryExecutionErrors.unsupportedFieldNameError(keyword)
        }
        dicomFile.values(i) match {
          case v: StringValue => writer.write(i, UTF8String.fromString(v.value))
          case v: UnsupportedValue =>
            throw new Exception(
              "Unsupported value of VR " ++ v.vr_name ++ " for keyword"
            )
        }
      }

    }

    Iterator.single(writer.getRow)
  }
}
