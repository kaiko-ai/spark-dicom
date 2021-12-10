package ai.kaiko.spark.sql.dicom

import ai.kaiko.dicom.DicomUtils
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.ObjectType
import org.dcm4che3.data.Attributes

import scala.reflect.ClassTag

object Encoders {
  implicit val ATTRIBUTES: Encoder[Attributes] = {
    val objSerializer: Expression = {
      val cls = classOf[Attributes]
      StaticInvoke(
        DicomUtils.getClass,
        BinaryType,
        "attrs2bytearray",
        BoundReference(0, ObjectType(cls), nullable = true) :: Nil,
        returnNullable = false
      )
    }
    val objDeserializer: Expression = {
      StaticInvoke(
        DicomUtils.getClass,
        ObjectType(classOf[Attributes]),
        "bytearray2attrs",
        GetColumnByOrdinal(0, BinaryType) :: Nil,
        returnNullable = false
      )
    }
    ExpressionEncoder(
      objSerializer,
      objDeserializer,
      clsTag = ClassTag(classOf[Attributes])
    )
  }
}
