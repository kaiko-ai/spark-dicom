package ai.kaiko.spark.dicom.deidentifier

import ai.kaiko.dicom.DicomStandardDictionary
import ai.kaiko.dicom.DicomDeidentifyDictionary
import org.apache.spark.sql.DataFrame
import org.dcm4che3.data._
import org.dcm4che3.data.VR._
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.LocalTime
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class DicomDeidAction(
  keyword: String,
  vr: VR,
  action: String
)

object DicomDeidentifier {

  val DUMMY_DATE = LocalDate.of(1, 1, 1).format(DateTimeFormatter.ISO_LOCAL_DATE)
  val DUMMY_TIME = LocalTime.of(0, 0, 0, 0).format(DateTimeFormatter.ISO_LOCAL_TIME)
  val DUMMY_DATE_TIME = LocalDateTime.of(LocalDate.of(1, 1, 1), LocalTime.of(0, 0, 0, 0)).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
  val DUMMY_ZERO_STRING = "0"
  val DUMMY_ZERO_INT = 0
  val DUMMY_EMPTY_STRING = ""
  val DUMMY_ANONYMIZED_STRING = "Anonymized"

  val vrReplaceMap = Map[VR, Either[Int, String]](
    LO -> Right(DUMMY_ANONYMIZED_STRING),
    SH -> Right(DUMMY_ANONYMIZED_STRING),
    PN -> Right(DUMMY_ANONYMIZED_STRING),
    CS -> Right(DUMMY_ANONYMIZED_STRING),
    DA -> Right(DUMMY_DATE),
    TM -> Right(DUMMY_TIME),
    DT -> Right(DUMMY_DATE_TIME),
    IS -> Right(DUMMY_ZERO_STRING),
    FD -> Left(DUMMY_ZERO_INT),
    FL -> Left(DUMMY_ZERO_INT),
    SS -> Left(DUMMY_ZERO_INT),
    US -> Left(DUMMY_ZERO_INT),
    ST -> Right(DUMMY_EMPTY_STRING)
  )

  val vrEmptyMap = Map[VR, Either[Int, String]](
    LO -> Right(DUMMY_EMPTY_STRING),
    SH -> Right(DUMMY_EMPTY_STRING),
    PN -> Right(DUMMY_EMPTY_STRING),
    CS -> Right(DUMMY_EMPTY_STRING),
    UI -> Right(DUMMY_EMPTY_STRING),
    DA -> Right(DUMMY_DATE),
    TM -> Right(DUMMY_TIME),
    DT -> Right(DUMMY_DATE_TIME),
    IS -> Right(DUMMY_ZERO_STRING),
    UL -> Left(DUMMY_ZERO_INT),
    ST -> Right(DUMMY_EMPTY_STRING)
  )

  val dropActions = List("X", "X/Z", "X/D", "X/Z/D", "X/Z/U*")
  val replaceActions = List("D", "Z", "Z/D", "D/X")

  def deidentify(dataframe: DataFrame): DataFrame = {

    val deidActions: Array[DicomDeidAction] = DicomDeidentifyDictionary.elements.flatMap(
      deidElem => DicomStandardDictionary.keywordMap.get(deidElem.keyword) match {
        case Some(stdElem) => stdElem.vr match {
          case Right(vr) => Some(DicomDeidAction(deidElem.keyword, vr, deidElem.action))
          case _ => None
        }
        case _ => None
      }
    )

    val deidActionGroups = deidActions.groupBy(_.action)

    val toKeep: Array[DicomDeidAction] = deidActionGroups.getOrElse("K", Array())
    val toEmpty = deidActionGroups.getOrElse("Z", Array()) ++ deidActionGroups.getOrElse("Z/D", Array())
    val toReplace = deidActionGroups.getOrElse("D", Array()) ++ deidActionGroups.getOrElse("D/X", Array())
    val toClean = deidActionGroups.getOrElse("C", Array())
    val toPseudonymize = deidActionGroups.getOrElse("U", Array())

    // reverse drop: select all columns not in deidActions
    val keepCols = (toKeep.map(_.keyword) ++ dataframe.columns diff deidActions.map(_.keyword)).map(col(_))
    val emptyCols = toEmpty.flatMap(deid => {
        vrEmptyMap.get(deid.vr) match {
          case Some(Left(dummyVal)) => Some(lit(dummyVal).as(deid.keyword))
          case Some(Right(dummyVal)) => Some(lit(dummyVal).as(deid.keyword))
          case _ => None
        }
      })
    val replaceCols = toReplace.flatMap(deid => {
        vrReplaceMap.get(deid.vr) match {
          case Some(Left(dummyVal)) => Some(lit(dummyVal).as(deid.keyword))
          case Some(Right(dummyVal)) => Some(lit(dummyVal).as(deid.keyword))
          case _ => None
        }
      })

    // implement later
    val cleanCols = toClean.map(deid => lit("ToClean").as(deid.keyword))
    val pseudonymizeCols = toPseudonymize.map(deid => lit("ToPseudonymize").as(deid.keyword))

    dataframe
      .select(keepCols: _*)
      .select(emptyCols ++ replaceCols ++ cleanCols ++ pseudonymizeCols: _*)
  }
}
