package ai.kaiko.spark.dicom

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.dcm4che3.data.Attributes
import org.dcm4che3.data.Tag
import org.dcm4che3.data.VR
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import ai.kaiko.dicom.data.ProxyAttributes

class TestDicomEncoders extends AnyFunSpec with WithSpark with Matchers {
  describe("Kryo") {
    it("loads Attributes to Spark") {
      implicit val encoder: Encoder[ProxyAttributes] =
        Encoders.kryo[ProxyAttributes]

      val someAttrs = new Attributes()
      someAttrs.setString(Tag.PatientName, VR.PN, "Guillaume")
      val someDataset = ProxyAttributes.from(someAttrs)

      val items: Seq[ProxyAttributes] = Seq(someDataset)

      val ds: Dataset[ProxyAttributes] = spark.createDataset(items)
      ds.count shouldBe 1

      // use some API
      val results =
        ds.filter(dataset => {
          val personName =
            ProxyAttributes.to(dataset).getString(Tag.PatientName)
          (personName != null && personName.startsWith("Gui"))
        })

      results.count shouldBe 1
      results.first shouldEqual someDataset
    }
  }
}
