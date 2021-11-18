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
