package ai.kaiko.spark.dicom.deidentifier

import ai.kaiko.dicom.DicomStandardDictionary
import org.apache.spark.sql.DataFrame
import org.dcm4che3.data.Keyword.{valueOf => keywordOf}
import org.dcm4che3.data._
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.LocalTime
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object DicomDeidentifier {

  val DUMMY_DATE = LocalDate.of(1, 1, 1).format(DateTimeFormatter.ISO_LOCAL_DATE)
  val DUMMY_TIME = LocalTime.of(0, 0, 0, 0).format(DateTimeFormatter.ISO_LOCAL_TIME)
  val DUMMY_DATE_TIME = LocalDateTime.of(LocalDate.of(1, 1, 1), LocalTime.of(0, 0, 0, 0)).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
  val DUMMY_ZERO_STRING = "0"
  val DUMMY_ZERO_INT = 0
  val DUMMY_EMPTY_STRING = ""
  val DUMMY_ANONYMIZED_STRING = "Anonymized"

  val KEYWORDS_TO_REPLACE = List(
    keywordOf(Tag.GraphicAnnotationSequence),
    keywordOf(Tag.PersonIdentificationCodeSequence),
    keywordOf(Tag.PersonName),
    keywordOf(Tag.VerifyingObserverName),
    keywordOf(Tag.VerifyingObserverSequence),
    keywordOf(Tag.AccessionNumber),
    keywordOf(Tag.ContentCreatorName),
    keywordOf(Tag.FillerOrderNumberImagingServiceRequest),
    keywordOf(Tag.PatientID),
    keywordOf(Tag.PatientBirthDate),
    keywordOf(Tag.PatientName),
    keywordOf(Tag.PatientSex),
    keywordOf(Tag.PlacerOrderNumberImagingServiceRequest),
    keywordOf(Tag.ReferringPhysicianName),
    keywordOf(Tag.StudyDate),
    keywordOf(Tag.StudyID),
    keywordOf(Tag.StudyTime),
    keywordOf(Tag.VerifyingObserverIdentificationCodeSequence),
  )

  def replace(dataframe: DataFrame): DataFrame = {
    import VR._

    var result = dataframe

    KEYWORDS_TO_REPLACE.foreach(keyword => {
      val vr = DicomStandardDictionary.keywordMap.get(keyword) match { 
        case Some(stdElem) => stdElem.vr 
        case _ =>
      }
      
      vr match {
        case Right(LO) | Right(SH) | Right(PN) | Right(CS) => result = result.withColumn(keyword, lit(DUMMY_ANONYMIZED_STRING))
        case Right(DA) => result = result.withColumn(keyword, lit(DUMMY_DATE))
        case Right(TM) => result = result.withColumn(keyword, lit(DUMMY_TIME))
        case Right(DT) => result = result.withColumn(keyword, lit(DUMMY_TIME))
        case Right(UI) => result = result.withColumn(keyword, md5(col(keyword)))
        case Right(IS) => result = result.withColumn(keyword, lit(DUMMY_ZERO_STRING))
        case Right(FD) | Right(FL) | Right(SS) | Right(US) => result = result.withColumn(keyword, lit(DUMMY_ZERO_INT))
        case Right(ST) => result = result.withColumn(keyword, lit(DUMMY_EMPTY_STRING))
        case _ =>
      }
    })
    result
  }

  def deid(dataframe: DataFrame): DataFrame = {
    var result = dataframe
    result = replace(result)
    result
  }
}
