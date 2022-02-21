// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package ai.kaiko.spark.dicom

import ai.kaiko.spark.dicom.v2.DicomDataSource
import org.apache.log4j.Priority
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger
import org.dcm4che3.data.Keyword.{valueOf => keywordOf}
import org.dcm4che3.data._
import org.dcm4che3.io.DicomOutputStream

import java.io.File
import java.time.LocalDate
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.nio.file.Files

/** Utils for testing the DICOM data source
  */
object DicomDataSourceTestUtils {
  val SOME_DICOM_FILEPATH =
    "src/test/resources/Pancreatic-CT-CBCT-SEG/Pancreas-CT-CB_001/07-06-2012-NA-PANCREAS-59677/201.000000-PANCREAS DI iDose 3-97846/1-001.dcm"
  lazy val SOME_DICOM_FILE = {
    val file = new File(SOME_DICOM_FILEPATH)
    assert(file.exists)
    file
  }
  val SOME_PATIENT_NAME = "Pancreas-CT-CB_001"
  val SOME_STUDY_NAME = "Pancreas-CT-CB_001"
  val SOME_STUDY_DATE = LocalDate.of(2012, 7, 6)
  val SOME_STUDY_TIME = LocalTime.of(11, 18, 23, 360000000)

  val SOME_DICOM_FOLDER_FILEPATH =
    "src/test/resources/Pancreatic-CT-CBCT-SEG/Pancreas-CT-CB_001/07-06-2012-NA-PANCREAS-59677/201.000000-PANCREAS DI iDose 3-97846/*"

  val SOME_NONDICOM_FILEPATH = "src/test/resources/nonDicom/test.txt"

  val SOME_META_ATTRS = {
    val attrs = new Attributes
    attrs.setValue(Tag.FileMetaInformationGroupLength, VR.UL, 204)
    attrs.setValue(
      Tag.FileMetaInformationVersion,
      VR.OB,
      Array[Byte](0x0, 0x1)
    )
    attrs.setValue(Tag.TransferSyntaxUID, VR.UI, UID.ExplicitVRLittleEndian)
    attrs
  }
}

class TestDicomDataSourceBatch extends SparkTest {
  import DicomDataSourceTestUtils._
  describe("Spark") {
    it("reads DICOM files") {
      val df = spark.read
        .format("dicomFile")
        .load(SOME_DICOM_FILEPATH)
        .select(
          col("path"),
          col(keywordOf(Tag.PatientName)),
          col(keywordOf(Tag.StudyDate)),
          col(keywordOf(Tag.StudyTime))
        )

      val row = df.first

      assert(
        row.getAs[Row](keywordOf(Tag.PatientName)).getAs[String](0)
          === SOME_STUDY_NAME
      )
      assert(
        row.getAs[String](keywordOf(Tag.StudyDate))
          === SOME_STUDY_DATE.format(
            DateTimeFormatter.ISO_LOCAL_DATE
          )
      )
      assert(
        row.getAs[String](keywordOf(Tag.StudyTime))
          === SOME_STUDY_TIME.format(
            DateTimeFormatter.ISO_LOCAL_TIME
          )
      )
    }

    it("reads all attributes of DICOM files") {
      // This test is successful if no exception is raised
      val df = spark.read
        .format("dicomFile")
        .load(SOME_DICOM_FILEPATH)
        .select("*")

      // Force evaluation of Dataset
      val row = df.first
    }

    describe("reads metadata") {
      it("path") {
        val df = spark.read
          .format("dicomFile")
          .load(SOME_DICOM_FILEPATH)
          .select("path")

        val row = df.first

        // abs vr rel path
        assert(row.getAs[String]("path").endsWith(SOME_DICOM_FILEPATH))
      }
      it("list of defined keywords") {
        val df = spark.read
          .format("dicomFile")
          .load(SOME_DICOM_FILEPATH)
          .select("keywords")

        val row = df.first
        val keywords = row.getList[String](row.fieldIndex("keywords"))

        assert(keywords.contains(keywordOf(Tag.PatientName)))
        assert(!keywords.contains(keywordOf(Tag.XRay3DAcquisitionSequence)))
      }
      it("mapping to concrete VR") {
        val df = spark.read
          .format("dicomFile")
          .load(SOME_DICOM_FILEPATH)
          .select("vrs")

        val vrs = df.first.getAs[Map[String, String]]("vrs")

        assert(vrs.get(keywordOf(Tag.PatientName)) === Some(VR.PN.name))
      }
    }

    it("collects parse errors") {
      // write some DICOM with the wrong format
      import java.nio.file.Files
      val tmpFile =
        Files.createTempFile("spark-dicom-test", "collect-parse-error.dcm")
      val tmpFilePath = tmpFile.toAbsolutePath.toUri.toString
      testLogger.log(Priority.INFO, f"Writing attribute file to $tmpFilePath")

      val someTag = Tag.StudyTime
      val someAttrs = {
        val attrs = new Attributes
        attrs.setString(someTag, VR.TM, "12:00:00")
        attrs
      }
      val dcmOutput = new DicomOutputStream(tmpFile.toFile)
      someAttrs.writeTo(dcmOutput)
      dcmOutput.flush

      val df = spark.read
        .format("dicomFile")
        .load(tmpFile.toAbsolutePath.toUri.toString)
        .select("errors", "StudyTime")

      val row = df.first

      assert {
        row
          .getAs[Map[String, String]]("errors")
          .get("StudyTime")
          .getOrElse(
            "No error"
          ) === "java.time.format.DateTimeParseException: Text '12:00:00' could not be parsed at index 2"
      }
    }

    it(
      "does not allow reading PixelData when not specified explicitly in config"
    ) {
      val thrown = intercept[org.apache.spark.sql.AnalysisException](
        spark.read
          .format("dicomFile")
          .load(SOME_DICOM_FILEPATH)
          .select(
            col("path"),
            col(keywordOf(Tag.PixelData))
          )
          .first
          .getAs[Array[Byte]](keywordOf(Tag.PixelData))
      )
      assert(thrown.message.startsWith("cannot resolve 'PixelData'"))
    }

    it("allows reading PixelData when specified explicitly in config") {
      assert(
        DicomDataSource
          .schema(
            includePixelData = true,
            includePrivateTags = false,
            includeContent = false
          )
          .fields
          .find(_.name == keywordOf(Tag.PixelData))
          .isDefined
      )
      assert(
        spark.read
          .format("dicomFile")
          .option(DicomDataSource.OPTION_INCLUDEPIXELDATA, true)
          .load(SOME_DICOM_FILEPATH)
          .select(
            col("path"),
            col(keywordOf(Tag.PixelData))
          )
          .first
          .getAs[Array[Byte]](keywordOf(Tag.PixelData))
          .size > 0
      )
    }

    it("allows reading whole content when specified explicitly in config") {
      assert(
        DicomDataSource
          .schema(
            includePixelData = false,
            includePrivateTags = false,
            includeContent = true
          )
          .fields
          .find(_.name == "content")
          .isDefined
      )
      assert(
        spark.read
          .format("dicomFile")
          .option(DicomDataSource.OPTION_INCLUDECONTENT, true)
          .load(SOME_DICOM_FILEPATH)
          .select(
            col("path"),
            col("content")
          )
          .first
          .getAs[Array[Byte]]("content")
          .size > 0
      )
    }
  }
}

class TestDicomDataSourcePrivateTags extends SparkTest {
  import DicomDataSourceTestUtils._
  describe("Spark") {
    it("allows reading private tags when specified explicitly in config") {
      assert(
        DicomDataSource
          .schema(
            includePixelData = false,
            includePrivateTags = true,
            includeContent = false
          )
          .fields
          .find(_.name == "privateTagsJson")
          .isDefined
      )

      // make a DICOM file with some private data for test
      val tmpFile =
        Files.createTempFile("spark-dicom-test", "privatetags.dcm")
      val tmpFilePath = tmpFile.toAbsolutePath.toUri.toString
      testLogger.log(Priority.INFO, f"Writing attribute file to $tmpFilePath")

      val someAttrs = {
        val attrs = new Attributes
        attrs.setString("someCreator", 0x00091000, VR.DA, "20220101")
        attrs
      }
      val dcmOutput = new DicomOutputStream(tmpFile.toFile)
      dcmOutput.writeDataset(SOME_META_ATTRS, someAttrs)
      dcmOutput.flush

      val row = spark.read
        .format("dicomFile")
        .option(DicomDataSource.OPTION_INCLUDEPRIVATETAGS, true)
        .load(tmpFile.toAbsolutePath.toUri.toString)
        .select(
          col("path"),
          col("privateTagsJson")
        )
        .first

      assert(
        row
          .getAs[String]("privateTagsJson")
          === "{\"00090010\":{\"vr\":\"LO\",\"Value\":[\"someCreator\"]},\"00091000\":{\"vr\":\"DA\",\"Value\":[\"20220101\"]}}"
      )
    }
  }
}

class TestDicomDataSourceStreaming extends SparkTest {
  import DicomDataSourceTestUtils._
  describe("Spark") {
    it("reads a stream of DICOM files") {
      val df = spark.readStream
        .schema(
          DicomDataSource.schema(
            includePixelData = false,
            includePrivateTags = false,
            includeContent = false
          )
        )
        .format("dicomFile")
        .load(SOME_DICOM_FOLDER_FILEPATH)
        .select(
          col("path"),
          col(f"${keywordOf(Tag.PatientName)}.Alphabetic")
            .as(keywordOf(Tag.PatientName)),
          col(keywordOf(Tag.StudyDate)),
          col(keywordOf(Tag.StudyTime))
        )

      val queryName = "testStreamDicom"
      val query = df.writeStream
        .trigger(Trigger.Once)
        .format("memory")
        .queryName(queryName)
        .start

      query.processAllAvailable
      val outDf =
        spark.table(queryName).select("path", keywordOf(Tag.PatientName))
      assert(outDf.count == 79)
    }

    it("doesn't fail upon reading a non-DICOM file") {
      val df = spark.read
        .format("dicomFile")
        .load(SOME_NONDICOM_FILEPATH)
        .select("path", "isDicom")

      val row = df.first

      assert(row.getAs[String]("path").endsWith(SOME_NONDICOM_FILEPATH))
      assert(!row.getAs[Boolean]("isDicom"))
    }
  }
}

class TestDicomDataSourceSequenceTags extends SparkTest {
  import DicomDataSourceTestUtils._
  describe("Spark") {
    it("allows reading sequence tags as JSON") {
      // make a DICOM file with some sequence data for test
      import java.nio.file.Files
      val tmpFile =
        Files.createTempFile("spark-dicom-test", "sequencetags.dcm")
      val tmpFilePath = tmpFile.toAbsolutePath.toUri.toString
      testLogger.log(Priority.INFO, f"Writing attribute file to $tmpFilePath")

      val someAttrs = {
        val attrs = new Attributes

        val seq = attrs.newSequence(Tag.DeidentificationMethodCodeSequence, 2)

        val nestedAttr1 = new Attributes
        nestedAttr1.setValue(Tag.DeidentificationMethod, VR.LO, "Method1")
        nestedAttr1.setValue(Tag.DeidentificationAction, VR.CS, "Action1")

        val nestedAttr2 = new Attributes
        nestedAttr2.setValue(Tag.DeidentificationMethod, VR.LO, "Method2")
        nestedAttr2.setValue(Tag.DeidentificationAction, VR.CS, "Action2")

        seq.add(nestedAttr1)
        seq.add(nestedAttr2)

        attrs
      }
      val dcmOutput = new DicomOutputStream(tmpFile.toFile)
      dcmOutput.writeDataset(SOME_META_ATTRS, someAttrs)
      dcmOutput.flush

      val row = spark.read
        .format("dicomFile")
        .load(tmpFile.toAbsolutePath.toUri.toString)
        .select(
          col("path"),
          col(keywordOf(Tag.DeidentificationMethodCodeSequence))
        )
        .first

      assert(
        row
          .getAs[String](keywordOf(Tag.DeidentificationMethodCodeSequence))
          === "[{\"00080307\":{\"vr\":\"CS\",\"Value\":[\"Action1\"]},\"00120063\":{\"vr\":\"LO\",\"Value\":[\"Method1\"]}},{\"00080307\":{\"vr\":\"CS\",\"Value\":[\"Action2\"]},\"00120063\":{\"vr\":\"LO\",\"Value\":[\"Method2\"]}}]"
      )
    }
    it("allows reading nested sequence tags as JSON") {
      // make a DICOM file with some sequence data for test
      val tmpFile =
        Files.createTempFile("spark-dicom-test", "nestedsequencetags.dcm")
      val tmpFilePath = tmpFile.toAbsolutePath.toUri.toString
      testLogger.log(Priority.INFO, f"Writing attribute file to $tmpFilePath")

      val someAttrs = {
        val attrs = new Attributes

        attrs.setValue(Tag.DeidentificationMethod, VR.LO, "UnrelatedToSequence")

        val seq = attrs.newSequence(Tag.DeidentificationMethodCodeSequence, 1)

        val nestedAttr = new Attributes
        nestedAttr.setValue(Tag.DeidentificationMethod, VR.LO, "Level1Method")

        val nestedSeq =
          nestedAttr.newSequence(Tag.DeidentificationActionSequence, 1)

        val nestedSeqAttr1 = new Attributes
        nestedSeqAttr1.setValue(
          Tag.DeidentificationAction,
          VR.CS,
          "Level2Action1"
        )

        val nestedSeqAttr2 = new Attributes
        nestedSeqAttr2.setValue(
          Tag.DeidentificationAction,
          VR.CS,
          "Level2Action2"
        )

        nestedSeq.add(nestedSeqAttr1)
        nestedSeq.add(nestedSeqAttr2)

        seq.add(nestedAttr)

        attrs
      }
      val dcmOutput = new DicomOutputStream(tmpFile.toFile)
      dcmOutput.writeDataset(SOME_META_ATTRS, someAttrs)
      dcmOutput.flush

      val row = spark.read
        .format("dicomFile")
        .load(tmpFile.toAbsolutePath.toUri.toString)
        .select(
          col("path"),
          col(keywordOf(Tag.DeidentificationMethodCodeSequence))
        )
        .first

      assert(
        row
          .getAs[String](keywordOf(Tag.DeidentificationMethodCodeSequence))
          === "[{\"00080305\":{\"vr\":\"SQ\",\"Value\":[{\"00080307\":{\"vr\":\"CS\",\"Value\":[\"Level2Action1\"]}},{\"00080307\":{\"vr\":\"CS\",\"Value\":[\"Level2Action2\"]}}]},\"00120063\":{\"vr\":\"LO\",\"Value\":[\"Level1Method\"]}}]"
      )
    }
    it("allows reading empty sequence tags as JSON") {
      // make a DICOM file with some sequence data for test
      val tmpFile =
        Files.createTempFile("spark-dicom-test", "nestedsequencetags.dcm")
      val tmpFilePath = tmpFile.toAbsolutePath.toUri.toString
      testLogger.log(Priority.INFO, f"Writing attribute file to $tmpFilePath")

      val someAttrs = {
        val attrs = new Attributes
        val seq = attrs.newSequence(Tag.DeidentificationMethodCodeSequence, 1)

        attrs
      }
      val dcmOutput = new DicomOutputStream(tmpFile.toFile)
      dcmOutput.writeDataset(SOME_META_ATTRS, someAttrs)
      dcmOutput.flush

      val row = spark.read
        .format("dicomFile")
        .load(tmpFile.toAbsolutePath.toUri.toString)
        .select(
          col("path"),
          col(keywordOf(Tag.DeidentificationMethodCodeSequence))
        )
        .first

      assert(
        row
          .getAs[String](keywordOf(Tag.DeidentificationMethodCodeSequence))
          === "[]"
      )
    }
  }
}
