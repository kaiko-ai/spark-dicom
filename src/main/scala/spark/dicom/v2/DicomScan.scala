package ai.kaiko.spark.dicom.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

case class DicomScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: Map[String, String],
    // TODO
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty
) extends FileScan {
  override def createReaderFactory(): PartitionReaderFactory = {
    // must be computed here as there isn't a Spark context anymore downstream
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(options)
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf)
    )
    DicomPartitionReaderFactory(
      dataSchema,
      readDataSchema,
      readPartitionSchema,
      broadcastedConf
    )
  }

  override def withFilters(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]
  ): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)

}
