package ai.kaiko.dicom

import scala.xml.XML
import org.dcm4che3.data.VR
import scala.util.Try

case class DicomStdElem(
    tag: Int,
    name: String,
    keyword: String,
    vr: Either[String, VR],
    vm: String,
    note: String
)

object DicomStandardDictionary {
  val DICOM_STD_XML_DOC_FILEPATH =
    "/dicom/stdDoc/part06.xml"

  lazy val elements: Array[DicomStdElem] = {
    val xmlResourceInputStream =
      Option(
        DicomStandardDictionary.getClass.getResourceAsStream(
          DICOM_STD_XML_DOC_FILEPATH
        )
      ).get
    val dicomStdXmlDoc = XML.load(xmlResourceInputStream)
    // find relevant xml table holding dict
    ((dicomStdXmlDoc \\ "book" \ "chapter" filter (elem =>
      elem \@ "label" == "6" ||
        elem \@ "label" == "7" ||
        elem \@ "label" == "8" ||
        elem \@ "label" == "9"
    )) \ "table" \ "tbody" \ "tr")
      // to Map entries
      .map(row => {
        // there is an invisible space in the texts, remove it
        val rowCellTexts = row \ "td" map (_.text.trim.replaceAll("​", ""))
        // we'll keep only std elements with valid hexadecimal tag
        Try(
          Integer.parseInt(
            rowCellTexts(0)
              .replace("(", "")
              .replace(")", "")
              .replace(",", ""),
            16
          )
        ).toOption.map(intTag =>
          DicomStdElem(
            tag = intTag,
            name = rowCellTexts(1),
            keyword = rowCellTexts(2),
            vr = {
              val vrStr = rowCellTexts(3)
              Try(VR.valueOf(vrStr)).toOption.toRight(vrStr)
            },
            vm = rowCellTexts(4),
            note = rowCellTexts(5)
          )
        )
      })
      .collect { case Some(v) if v.keyword.nonEmpty => v }
      .toArray
  }

  lazy val keywordMap: Map[String, DicomStdElem] =
    elements.map(stdElem => stdElem.keyword -> stdElem).toMap
}
