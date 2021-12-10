package ai.kaiko.spark.dicom

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.SparkSession
import org.dcm4che3.data.Attributes
import org.dcm4che3.data.Tag
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class TestDicomEncoders extends AnyFunSpec with Matchers {
  val SOME_DICOM_FILEPATH =
    "src/test/resources/Pancreatic-CT-CBCT-SEG/Pancreas-CT-CB_001/07-06-2012-NA-PANCREAS-59677/201.000000-PANCREAS DI iDose 3-97846/1-001.dcm"
  val SOME_DICOM_PATIENT_NAME = "Pancreas-CT-CB_001"

  val spark = SparkSession.builder.master("local").getOrCreate

  val df =
    spark.read.format("binaryFile").load(SOME_DICOM_FILEPATH).select("content")

  assert(df.count == 1)

  describe("Spark") {
    describe("native") {
      implicit val encoder: Encoder[Attributes] =
        org.apache.spark.sql.Encoders.kryo[Attributes]

      it("does not allow to manipulate DICOM content as dcm4che Attributes") {
        // mapping with identity forces evaluation of encoder
        (df.as[Attributes].map(identity).first) shouldBe null
      }
    }

    describe("with kaiko's spark-dicom") {
      implicit val encoder: Encoder[Attributes] =
        ai.kaiko.spark.sql.dicom.Encoders.ATTRIBUTES

      it("allows to manipulate DICOM content as dcm4che Attributes") {
        df.as[Attributes]
          .map(identity)
          .first
          .getString(Tag.PatientName) shouldBe SOME_DICOM_PATIENT_NAME
      }
    }
  }
}
