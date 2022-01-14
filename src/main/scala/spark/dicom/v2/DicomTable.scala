// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package ai.kaiko.spark.dicom.v2

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import scala.util.Try

case class DicomTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat]
) extends FileTable(sparkSession, options, paths, userSpecifiedSchema)
    with Logging {

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    val withPixelData: Boolean =
      options.asScala.toMap
        .get(DicomDataSource.OPTION_WITHPIXELDATA.toLowerCase)
        .flatMap(b => Try(b.toBoolean).toOption)
        .getOrElse(false)
    Some(DicomDataSource.schema(withPixelData))
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    DicomScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    throw new NotImplementedError("Writing to files is not supported")

  override def supportsDataType(dataType: DataType): Boolean = dataType match {
    // accept all data types for now
    case _ => true
  }

  override def formatName: String = "DICOM"
}
