package ai.kaiko.dicom

import scala.util.Try
import scala.xml.XML
import org.dcm4che3.data.Keyword

case class DicomDeidElem(
  tag: Int,
  name: String,
  keyword: String,
  action: String
)

object DicomDeidentifyDictionary {
  val DICOM_DEID_XML_DOC_FILEPATH =
    "/dicom/stdDoc/part15.xml"

  lazy val elements: Array[DicomDeidElem] = {
    val xmlResourceInputStream =
      Option(
        DicomDeidentifyDictionary.getClass.getResourceAsStream(
          DICOM_DEID_XML_DOC_FILEPATH
        )
      ).get
      val dicomDeidXmlDoc = XML.load(xmlResourceInputStream)
      // find relevant xml table holding dict
      ((dicomDeidXmlDoc \\ "table" filter (elem =>
        elem \@ "label" == "E.1-1"
      )) \ "tbody" \ "tr")
      // to Map entries
      .map(row => {
        // there is an invisible space in the texts, remove it
        val rowCellTexts = row \ "td" map (_.text.trim.replaceAll("​", ""))
        // we'll keep only std elements with valid hexadecimal tag
        Try(
          Integer.parseInt(
            rowCellTexts(1)
              .replace("(", "")
              .replace(")", "")
              .replace(",", ""),
            16
          )
        ).toOption.map(intTag =>
          DicomDeidElem(
            tag = intTag,
            name = rowCellTexts(0),
            keyword = Keyword.valueOf(intTag),
            action = rowCellTexts(4)
          )
        )
      })
      .collect { case Some(v) if v.name.nonEmpty => v }
      .toArray
  }

  lazy val keywordMap: Map[String, DicomDeidElem] =
    elements.map(deidElem => deidElem.keyword -> deidElem).toMap

  lazy val tagMap: Map[Int, DicomDeidElem] =
    elements.map(deidElem => deidElem.tag -> deidElem).toMap

}