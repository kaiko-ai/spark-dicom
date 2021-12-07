package ai.kaiko.spark.dicom

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.dcm4che3.data.Attributes
import org.dcm4che3.data.Tag
import org.dcm4che3.data.VR
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class TestDicomEncoders extends AnyFunSpec with WithSpark with Matchers {
  describe("Kryo") {
    it("loads Attributes to Spark") {
      implicit val encoder: Encoder[Attributes] = Encoders.kryo[Attributes]

      val someAttrs = new Attributes()
      someAttrs.setString(Tag.PersonName, VR.PN, "Guillaume")

      val items: Seq[Attributes] = Seq(someAttrs)

      val ds: Dataset[Attributes] = spark.createDataset(items)
      ds.count shouldBe 1

      val results =
        ds.filter(attrs => {
          System.out.println(
            "TAGS=" + attrs.tags.map(_.toHexString).mkString(",")
          )
          val personName = attrs.getString(Tag.PersonName)
          (personName != null && personName.startsWith("Gui"))
        })

      results.count shouldBe 1
      results.first shouldBe someAttrs
    }
  }
}
