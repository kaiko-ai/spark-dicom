package ai.kaiko.spark.dicom.deidentifier

import ai.kaiko.dicom.DicomStandardDictionary
import org.apache.spark.sql.DataFrame
import org.dcm4che3.data.Keyword.{valueOf => keywordOf}
import org.dcm4che3.data._
import org.dcm4che3.data.VR._
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

  val TAGS_TO_ACTIONS = Map[String, (DataFrame, String, VR) => DataFrame](
    keywordOf(Tag.GraphicAnnotationSequence) -> replace,
    keywordOf(Tag.PersonIdentificationCodeSequence) -> replace,
    keywordOf(Tag.PersonName) -> replace,
    keywordOf(Tag.VerifyingObserverName) -> replace,
    keywordOf(Tag.VerifyingObserverSequence) -> replace,
    keywordOf(Tag.AccessionNumber) -> replace,
    keywordOf(Tag.ContentCreatorName) -> replace,
    keywordOf(Tag.FillerOrderNumberImagingServiceRequest) -> replace,
    keywordOf(Tag.PatientID) -> replace,
    keywordOf(Tag.PatientBirthDate) -> replace,
    keywordOf(Tag.PatientName) -> replace,
    keywordOf(Tag.PatientSex) -> replace,
    keywordOf(Tag.PlacerOrderNumberImagingServiceRequest) -> replace,
    keywordOf(Tag.ReferringPhysicianName) -> replace,
    keywordOf(Tag.StudyDate) -> replace,
    keywordOf(Tag.StudyID) -> replace,
    keywordOf(Tag.StudyTime) -> replace,
    keywordOf(Tag.VerifyingObserverIdentificationCodeSequence) -> replace,
  )

  def replace(dataframe: DataFrame, keyword: String, vr: VR): DataFrame = {

    vr match {
      case LO | SH | PN | CS => dataframe.withColumn(keyword, lit(DUMMY_ANONYMIZED_STRING))
      case DA => dataframe.withColumn(keyword, lit(DUMMY_DATE))
      case TM => dataframe.withColumn(keyword, lit(DUMMY_TIME))
      case DT => dataframe.withColumn(keyword, lit(DUMMY_TIME))
      case UI => dataframe.withColumn(keyword, md5(col(keyword)))
      case IS => dataframe.withColumn(keyword, lit(DUMMY_ZERO_STRING))
      case FD | FL | SS | US => dataframe.withColumn(keyword, lit(DUMMY_ZERO_INT))
      case ST => dataframe.withColumn(keyword, lit(DUMMY_EMPTY_STRING))
      case _ => dataframe
    }
  }

  def deidentify(dataframe: DataFrame): DataFrame = {
    var result = dataframe

    TAGS_TO_ACTIONS.foreach( keywordAction => {
      val vr = DicomStandardDictionary.keywordMap.get(keywordAction._1) match { 
        case Some(stdElem) => stdElem.vr match {
          case Right(vr) => result = keywordAction._2(result, keywordAction._1, vr)
          case _ => 
        } 
        case _ =>
      }
    })
    result
  }
}
