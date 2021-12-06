package ai.kaiko.spark.dicom.v2

import ai.kaiko.spark.dicom.DicomFileFormat
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import collection.JavaConverters._

case class DicomTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat]
) extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  override def newScanBuilder(
      options: CaseInsensitiveStringMap
  ): ScanBuilder = {
    // turn options into a Scala Map because config is, in fact, not case insensitive
    val optionsMap: Map[String, String] =
      options.asCaseSensitiveMap.asScala.toMap
    DicomScanBuilder(sparkSession, fileIndex, schema, dataSchema, optionsMap)
  }

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] =
    Some(DicomFileFormat.SCHEMA)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new WriteBuilder {
      override def build(): Write =
        DicomWrite(paths, formatName, supportsDataType, info)
    }

  override def supportsDataType(dataType: DataType): Boolean = dataType match {
    // accept all data types for now
    case _ => true
  }

  override def formatName: String = "DICOM"
}
