package ai.kaiko.spark.dicom

import ai.kaiko.dicom.DicomStandardDictionary
import org.apache.spark.sql.types._

object DicomStandardSpark {
  lazy val fields: Array[StructField] = {
    DicomStandardDictionary.elements
      .collect {
        case stdElem if stdElem.vr.isRight =>
          StructField(
            stdElem.keyword,
            DicomSparkMapper.from(stdElem.vr.toOption.get).sparkDataType,
            nullable = true
          )
      }
  }
  lazy val schema: StructType = new StructType(fields)
}
