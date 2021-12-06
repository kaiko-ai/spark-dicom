package ai.kaiko.spark.dicom

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.dcm4che3.data.Attributes
import org.dcm4che3.data.Tag
import org.dcm4che3.data.VR
import org.scalatest.funspec.AnyFunSpec
import org.apache.spark.sql.Encoders

class TestDicomEncoders extends AnyFunSpec with WithSpark {
  describe("DicomEncoders") {
    it("loads Attributes to Spark") {
      implicit val encoder: Encoder[Attributes] = Encoders.kryo[Attributes]

      val someAttrs = new Attributes()
      someAttrs.setString(Tag.PersonName, VR.PN, "Guillaume")

      val items: Seq[Attributes] = Seq(someAttrs)

      val df: Dataset[Attributes] =
        spark.createDataset(items)

      df.show
    }
  }
}
