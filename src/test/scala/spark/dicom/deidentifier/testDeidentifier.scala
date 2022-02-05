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
package ai.kaiko.spark.dicom.deidentifier

import ai.kaiko.dicom.ActionCode
import ai.kaiko.dicom.DicomDeidElem
import ai.kaiko.dicom.DicomDeidentifyDictionary.DUMMY_DATE
import ai.kaiko.dicom.DicomDeidentifyDictionary.DUMMY_DATE_TIME
import ai.kaiko.dicom.DicomDeidentifyDictionary.DUMMY_STRING
import ai.kaiko.dicom.DicomDeidentifyDictionary.DUMMY_TIME
import ai.kaiko.dicom.DicomDeidentifyDictionary.EMPTY_STRING
import ai.kaiko.spark.dicom.WithSpark
import ai.kaiko.spark.dicom.deidentifier.DicomDeidentifier._
import ai.kaiko.spark.dicom.deidentifier.options._
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.dcm4che3.data.Keyword.{valueOf => keywordOf}
import org.dcm4che3.data._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.CancelAfterFailure
import org.scalatest.funspec.AnyFunSpec

import java.io.File

object TestDicomDeidentifier {
  val SOME_DICOM_FILEPATH =
    "src/test/resources/Pancreatic-CT-CBCT-SEG/Pancreas-CT-CB_001/07-06-2012-NA-PANCREAS-59677/201.000000-PANCREAS DI iDose 3-97846/1-001.dcm"
  lazy val SOME_DICOM_FILE = {
    val file = new File(SOME_DICOM_FILEPATH)
    assert(file.exists)
    file
  }
}

class TestDicomDeidentifier
    extends AnyFunSpec
    with BeforeAndAfterAll
    with CancelAfterFailure
    with WithSpark {
  import TestDicomDeidentifier._

  val logger = {
    val logger = LogManager.getLogger(getClass.getName);
    logger.setLevel(Level.DEBUG)
    logger
  }

  val SOME_DROPPED_COL = keywordOf(Tag.ContainerComponentID)
  val SOME_DUMMY_DATE_COL = keywordOf(Tag.ContentDate)
  val SOME_DUMMY_TIME_COL = keywordOf(Tag.ContentTime)
  val SOME_DUMMY_DATE_TIME_COL = keywordOf(Tag.AttributeModificationDateTime)
  val SOME_DUMMY_STRING_COL = keywordOf(Tag.AnnotationGroupLabel)
  val SOME_ZERO_INT_COL = keywordOf(Tag.PregnancyStatus)
  val SOME_ZERO_STRING_COL = keywordOf(Tag.ConceptualVolumeDescription)
  val SOME_EMPTY_STRING_COL = keywordOf(Tag.AccessionNumber)
  val SOME_CLEANED_COL = keywordOf(Tag.AcquisitionComments)

  describe("Spark") {
    it("Deidentifies DICOM dataframe using basic profile") {
      var df = spark.read
        .format("dicomFile")
        .load(SOME_DICOM_FILEPATH)

      df = deidentify(df)

      val row = df.first

      assert(
        row.getAs[String](SOME_DUMMY_DATE_COL)
          === DUMMY_DATE
      )
      assert(
        row.getAs[String](SOME_DUMMY_TIME_COL)
          === DUMMY_TIME
      )
      assert(
        row.getAs[String](SOME_DUMMY_DATE_TIME_COL)
          === DUMMY_DATE_TIME
      )
      assert(
        row.getAs[String](SOME_DUMMY_STRING_COL)
          === DUMMY_STRING
      )
      assert(
        row.getAs[String](SOME_EMPTY_STRING_COL)
          === EMPTY_STRING
      )
      assertThrows[IllegalArgumentException] {
        row.fieldIndex(SOME_DROPPED_COL)
      }
      // No zero string/int cols in basic profile. To be added later
      // assert(
      //   row.getAs[Int](SOME_ZERO_INT_COL)
      //     === ZERO_INT
      // )
      // assert(
      //   row.getAs[String](SOME_ZERO_STRING_COL)
      //     === ZERO_STRING
      // )
    }
    it("Deidentifies DICOM dataframe using an option") {
      var df = spark.read
        .format("dicomFile")
        .load(SOME_DICOM_FILEPATH)

      val config: Map[DeidOption, Boolean] = Map(CleanDesc -> true)

      df = deidentify(df, config)

      val row = df.first

      assert(
        row.getAs[String](SOME_CLEANED_COL)
          === "ToClean"
      )
    }
  }
  describe("Deidentifier") {
    it("getAction returns default action when no options given") {
      val deidElem = DicomDeidElem(
        tag = 0,
        name = "test",
        keyword = "test",
        action = ActionCode.X,
        deidOptionToAction = Map(RetainUids -> ActionCode.D)
      )
      assert(
        DicomDeidentifier.getAction(
          deidElem,
          VR.ST
        ) === Drop()
      )
    }
    it("getAction returns correct action when option is given") {
      val deidElem = DicomDeidElem(
        tag = 0,
        name = "test",
        keyword = "test",
        action = ActionCode.X,
        deidOptionToAction = Map(RetainUids -> ActionCode.D)
      )
      val config: Map[DeidOption, Boolean] = Map(RetainUids -> true)
      assert(
        DicomDeidentifier.getAction(deidElem, VR.LO, config) === Dummify(
          Some(DUMMY_STRING)
        )
      )
    }
    it("getAction returns correct action when multiple options are given") {
      val deidElem = DicomDeidElem(
        tag = 0,
        name = "test",
        keyword = "test",
        action = ActionCode.X,
        deidOptionToAction = Map(
          RetainUids -> ActionCode.D,
          RetainDevId -> ActionCode.Z
        )
      )
      val config: Map[DeidOption, Boolean] =
        Map(RetainUids -> true, RetainDevId -> true)
      assert(
        DicomDeidentifier.getAction(deidElem, VR.LO, config) === Empty(
          Some(EMPTY_STRING)
        )
      )
    }
    it(
      "getAction returns correct action when multiple (irrelevant) options are given"
    ) {
      val deidElem = DicomDeidElem(
        tag = 0,
        name = "test",
        keyword = "test",
        action = ActionCode.X,
        deidOptionToAction = Map(
          RetainUids -> ActionCode.D,
          RetainDevId -> ActionCode.Z
        )
      )
      val config: Map[DeidOption, Boolean] = Map(
        RetainUids -> true,
        RetainDevId -> true,
        RetainLongFullDates -> true,
        RetainLongModifDates -> true
      )
      assert(
        DicomDeidentifier.getAction(deidElem, VR.LO, config) === Empty(
          Some(EMPTY_STRING)
        )
      )
    }
  }
  describe("DeidActions") {
    it("Pseudonymize makeDeidentifiedColumn makes a pseudonym of uid value") {
      val SOME_UID_COLUMN = keywordOf(Tag.ConcatenationUID)
      val SOME_UID_VAL = "uidValue"

      val data = Seq(Row(SOME_UID_VAL))
      val schema = StructType(Seq(StructField(SOME_UID_COLUMN, StringType)))
      var df =
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      df = deidentify(df)

      val row = df.first

      assert(df.first.getAs[String](SOME_UID_COLUMN) !== SOME_UID_VAL)
    }
    it("Pseudonymize makeDeidentifiedColumn does not change empty string") {
      val SOME_UID_COLUMN = keywordOf(Tag.ConcatenationUID)

      val data = Seq(Row(EMPTY_STRING))
      val schema = StructType(Seq(StructField(SOME_UID_COLUMN, StringType)))
      var df =
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      df = deidentify(df)

      val row = df.first

      assert(df.first.getAs[String](SOME_UID_COLUMN) === EMPTY_STRING)
    }
    it(
      "Pseudonymize makeDeidentifiedColumn is deterministic when salt is given"
    ) {
      val SOME_UID_COLUMN = keywordOf(Tag.ConcatenationUID)
      val SOME_UID_VAL = "uidValue"
      val SOME_SALT = "aSaltValue"

      val data = Seq(Row(SOME_UID_VAL))
      val schema = StructType(Seq(StructField(SOME_UID_COLUMN, StringType)))
      val df =
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      val firstRun = deidentify(df, salt = SOME_SALT)
      val firstHash = firstRun.first.getAs[String](SOME_UID_COLUMN)

      val secondRun = deidentify(df, salt = SOME_SALT)
      val secondHash = secondRun.first.getAs[String](SOME_UID_COLUMN)

      assert(firstHash === secondHash)
    }
    it(
      "Pseudonymize makeDeidentifiedColumn results differ with different salt"
    ) {
      val SOME_UID_COLUMN = keywordOf(Tag.ConcatenationUID)
      val SOME_UID_VAL = "uidValue"
      val FIRST_SALT = "aFirstSaltValue"
      val SECOND_SALT = "aSecondSaltValue"

      val data = Seq(Row(SOME_UID_VAL))
      val schema = StructType(Seq(StructField(SOME_UID_COLUMN, StringType)))
      val df =
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      val firstRun = deidentify(df, salt = FIRST_SALT)
      val firstHash = firstRun.first.getAs[String](SOME_UID_COLUMN)

      val secondRun = deidentify(df, salt = SECOND_SALT)
      val secondHash = secondRun.first.getAs[String](SOME_UID_COLUMN)

      assert(firstHash !== secondHash)
    }
  }
}
