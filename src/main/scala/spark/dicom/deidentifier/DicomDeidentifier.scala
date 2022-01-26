package ai.kaiko.spark.dicom.deidentifier

import ai.kaiko.dicom.DicomStandardDictionary
import ai.kaiko.dicom.DicomDeidentifyDictionary
import org.apache.spark.sql.DataFrame
import org.dcm4che3.data._
import org.apache.spark.sql.functions._


case class DicomDeidAction(
  keyword: String,
  vr: VR,
  action: String
)

object DicomDeidentifier {

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

    val toKeep = deidActionGroups.getOrElse("K", Array())
    val toEmpty = deidActionGroups.getOrElse("Z", Array()) ++ deidActionGroups.getOrElse("Z/D", Array())
    val toReplace = deidActionGroups.getOrElse("D", Array()) ++ deidActionGroups.getOrElse("D/X", Array())
    val toClean = deidActionGroups.getOrElse("C", Array())
    val toPseudonymize = deidActionGroups.getOrElse("U", Array())
    val toDrop = 
      deidActionGroups.getOrElse("X", Array()) ++ 
      deidActionGroups.getOrElse("X/Z", Array()) ++ 
      deidActionGroups.getOrElse("X/D", Array()) ++
      deidActionGroups.getOrElse("X/Z/D", Array()) ++
      deidActionGroups.getOrElse("X/Z/U*", Array())

    val keepCols = toKeep.map(deid => col(deid.keyword))
    val emptyCols = toEmpty.flatMap(deid => {
        DicomDeidentifyDictionary.getEmptyValue(deid.vr) match {
          case Some(emptyVal) => Some(lit(emptyVal).as(deid.keyword))
          case _ => None
        }
      })
    val replaceCols = toReplace.flatMap(deid => {
        DicomDeidentifyDictionary.getDummyValue(deid.vr) match {
          case Some(dummyVal) => Some(lit(dummyVal).as(deid.keyword))
          case _ => None
        }
      })

    // implement later
    val cleanCols = toClean.map(deid => lit("ToClean").as(deid.keyword))
    val pseudonymizeCols = toPseudonymize.map(deid => lit("ToPseudonymize").as(deid.keyword))

    val dropCols = toDrop.map(_.keyword)

    dataframe
      .select(col("*") +: (keepCols ++ emptyCols ++ replaceCols ++ cleanCols ++ pseudonymizeCols): _*)
      .drop(dropCols: _*)
  }
}
