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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.StructFilters
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.connector.read.SupportsPushDownFilters
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

object DicomConf {
  val DICOM_FILTER_PUSHDOWN_ENABLED = SQLConf
    .buildConf("spark.sql.dicom.filterPushdown.enabled")
    .doc("When true, enable filter pushdown to DICOM datasource.")
    .version("3.2.0")
    .booleanConf
    .createWithDefault(true)
}

case class DicomScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap
) extends FileScanBuilder(sparkSession, fileIndex, dataSchema)
    with SupportsPushDownFilters {

  override def build(): Scan = DicomScan(
    sparkSession,
    fileIndex,
    dataSchema,
    readDataSchema(),
    readPartitionSchema(),
    options,
    pushedFilters()
  )
  private var _pushedFilters: Array[Filter] = Array.empty

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    if (
      sparkSession.sessionState.conf.getConf(
        DicomConf.DICOM_FILTER_PUSHDOWN_ENABLED
      )
    ) {
      _pushedFilters = StructFilters.pushedFilters(filters, dataSchema)
    }
    filters
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters
}
