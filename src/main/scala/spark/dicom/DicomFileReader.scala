package ai.kaiko.spark.dicom

import ai.kaiko.dicom.DateValue
import ai.kaiko.dicom.DicomHelper
import ai.kaiko.dicom.DicomValue
import ai.kaiko.dicom.IntValue
import ai.kaiko.dicom.StringValue
import ai.kaiko.dicom.TimeValue
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
import org.dcm4che3.data.VR
import org.dcm4che3.io.DicomInputStream

import java.net.URI

object DicomFileReader {

  /** Make a Spark StructField out of a tag keyword and a VR when possible
    *
    * @param keyword
    *   keyword of the DICOM tag, see [[Keyword.valueOf]] to get it
    * @param vr
    *   VR of the corresponding tag
    * @return
    *   Some Spark StructField where applicable
    */
  def maybeBuildSparkStructFieldFrom(keyword: String, vr: VR) = {
    // https://dicom.nema.org/dicom/2013/output/chtml/part05/sect_6.2.html#table_6.2-1
    vr match {
      case VR.AE => Some(StructField(keyword, StringType))
      case VR.AS => Some(StructField(keyword, StringType))
      case VR.AT => Some(StructField(keyword, IntegerType))
      case VR.CS => Some(StructField(keyword, StringType))
      case VR.DA => Some(StructField(keyword, StringType))
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
      case VR.PN => Some(StructField(keyword, StringType))
      case VR.SH => None
      case VR.SL => None
      case VR.SQ => None
      case VR.SS => None
      case VR.ST => None
      case VR.SV => None
      case VR.TM => Some(StructField(keyword, StringType))
      case VR.UC => None
      case VR.UI => None
      case VR.UL => None
      case VR.UN => None
      case VR.UR => None
      case VR.US => None
      // Unlimited Text
      case VR.UT => Some(StructField(keyword, StringType))
      case VR.UV => None
    }
  }

  def inferSchema(
      conf: Configuration,
      file: FileStatus,
      includeDefault: Boolean
  ): StructType = {
    val fs = file.getPath.getFileSystem(conf)
    val fileStream = fs.open(file.getPath)
    val dicomInputStream = new DicomInputStream(fileStream)

    // read data
    val attrs = dicomInputStream.readDataset

    // build struct fields
    val fields = attrs.tags
      .map { case (tag) =>
        maybeBuildSparkStructFieldFrom(
          DicomHelper.standardDictionary.keywordOf(tag),
          DicomHelper.standardDictionary.vrOf(tag)
        )
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
      // allow getting a binary dump
      case (DicomFileFormat.CONTENT, i) => {
        val fileStream = fs.open(status.getPath)
        val dicomInputStream = new DicomInputStream(fileStream)
        try {
          writer.write(i, ByteStreams.toByteArray(dicomInputStream))
        } finally {
          Closeables.close(fileStream, true)
          Closeables.close(dicomInputStream, true)
        }
      }
      // any other requested field should be a DICOM keyword
      case (keyword, i) => {
        val tagOfKeyword =
          DicomHelper.standardDictionary.tagForKeyword(keyword)
        if (tagOfKeyword == -1) {
          throw QueryExecutionErrors.unsupportedFieldNameError(keyword)
        }

        DicomValue.readDicomValue(attrs, tagOfKeyword) match {
          case v: IntValue => writer.write(i, v.value)
          case v: StringValue => {
            writeString(i, v.value)
          }
          case v: DateValue => {
            writeString(i, v.value.format(DateValue.SPARK_FORMATTER))
          }
          case v: TimeValue =>
            writeString(i, v.value.format(TimeValue.SPARK_FORMATTER))
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
