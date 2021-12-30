package ai.kaiko.spark.dicom

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.dcm4che3.data._

import java.time.LocalDate
import java.time.LocalTime
import java.time.format.DateTimeFormatter

case class DicomSparkMapper(
    sparkDataType: DataType,
    reader: (Attributes, Int) => Any
)

object DicomSparkMapper {
  lazy val DEFAULT_MAPPER = DicomSparkMapper(
    sparkDataType = BinaryType,
    reader = _.getBytes(_)
  )

  def from(vr: VR): DicomSparkMapper = {
    import VR._
    vr match {
      case AE | AS | AT | CS | DS | DT | IS | LO | LT | SH | ST | UC | UI | UR |
          UT =>
        DicomSparkMapper(
          sparkDataType = StringType,
          reader = (attrs, tag) =>
            UTF8String.fromString(Option(attrs.getString(tag)).getOrElse(""))
        )
      case PN =>
        DicomSparkMapper(
          sparkDataType = new StructType(
            Array(
              new StructField("Alphabetic", StringType, true),
              new StructField("Ideographic", StringType, true),
              new StructField("Phonetic", StringType, true)
            )
          ),
          reader = (attrs, tag) => {
            val personName = new PersonName(attrs.getString(tag), true)
            InternalRow(
              UTF8String.fromString(
                Option(personName.toString(PersonName.Group.Alphabetic, true))
                  .getOrElse("")
              ),
              UTF8String.fromString(
                Option(
                  personName.toString(PersonName.Group.Ideographic, true)
                ).getOrElse("")
              ),
              UTF8String.fromString(
                Option(personName.toString(PersonName.Group.Phonetic, true))
                  .getOrElse("")
              )
            )
          }
        )
      case FL | FD =>
        DicomSparkMapper(
          sparkDataType = ArrayType(DoubleType, false),
          reader = _.getDoubles(_)
        )
      case SL | SS | US | UL =>
        DicomSparkMapper(
          sparkDataType = ArrayType(IntegerType, false),
          reader = _.getInts(_)
        )
      case SV | UV =>
        DicomSparkMapper(
          sparkDataType = ArrayType(LongType, false),
          reader = _.getLongs(_)
        )
      case DA =>
        DicomSparkMapper(
          sparkDataType = StringType,
          reader = (attrs, tag) =>
            UTF8String.fromString(
              Option(attrs.getString(tag))
                .map(
                  LocalDate
                    .parse(
                      _,
                      DateTimeFormatter
                        .ofPattern("yyyyMMdd")
                    )
                    .format(DateTimeFormatter.ISO_LOCAL_DATE)
                )
                .getOrElse("")
            )
        )
      case TM =>
        DicomSparkMapper(
          sparkDataType = StringType,
          reader = (attrs, tag) =>
            UTF8String.fromString(
              Option(attrs.getString(tag))
                .map(
                  LocalTime
                    .parse(
                      _,
                      DateTimeFormatter
                        .ofPattern("HHmmss[.SSS][.SS][.S]")
                    )
                    .format(DateTimeFormatter.ISO_LOCAL_TIME)
                )
                .getOrElse("")
            )
        )
      // map others to binary for the time being
      case _ => DEFAULT_MAPPER
    }
  }
}
