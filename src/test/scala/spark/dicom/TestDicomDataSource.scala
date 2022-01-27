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
import ai.kaiko.spark.WithSpark
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.log4j.Priority
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger
import org.dcm4che3.data.Keyword.{valueOf => keywordOf}
import org.dcm4che3.data._
import org.dcm4che3.io.DicomOutputStream
import org.scalatest.CancelAfterFailure
import org.scalatest.DoNotDiscover
import org.scalatest.funspec.AnyFunSpec

import java.io.File
import java.time.LocalDate
import java.time.LocalTime
import java.time.format.DateTimeFormatter

object TestDicomDataSource {
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
}

@DoNotDiscover
class TestDicomDataSource
    extends AnyFunSpec
    with WithSpark
    with CancelAfterFailure {
  import TestDicomDataSource._

  val logger = {
    val logger = LogManager.getLogger(getClass.getName);
    logger.setLevel(Level.DEBUG)
    logger
  }

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
      logger.log(Priority.INFO, f"Writing attribute file to $tmpFilePath")

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
          .schema(withPixelData = true)
          .fields
          .find(_.name == keywordOf(Tag.PixelData))
          .isDefined
      )
      assert(
        spark.read
          .format("dicomFile")
          .option(DicomDataSource.OPTION_WITHPIXELDATA, true)
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

    it("reads a stream of DICOM files") {
      val df = spark.readStream
        .schema(DicomDataSource.schema(false))
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
