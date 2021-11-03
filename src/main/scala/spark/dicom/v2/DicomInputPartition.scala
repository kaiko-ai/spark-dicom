package spark.dicom.v2

import org.apache.spark.sql.connector.read.InputPartition

class DicomInputPartition extends InputPartition {
  override def preferredLocations(): Array[String] = Array.empty
}
