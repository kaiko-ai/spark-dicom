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

import ai.kaiko.spark.dicom.deidentifier.DicomDeidentifier._
import ai.kaiko.dicom.DicomDeidentifyDictionary.{
  DUMMY_DATE,
  DUMMY_TIME,
  DUMMY_DATE_TIME,
  EMPTY_STRING,
  DUMMY_STRING
}
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.dcm4che3.data.Keyword.{valueOf => keywordOf}
import org.dcm4che3.data._
import org.scalatest.CancelAfterFailure
import org.scalatest.funspec.AnyFunSpec
import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.scalatest.BeforeAndAfterAll

trait WithSpark {
  var spark = {
    val spark = SparkSession.builder.master("local").getOrCreate
    spark.sparkContext.setLogLevel(Level.ERROR.toString())
    spark
  }
}

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

  override protected def afterAll(): Unit = {
    spark.stop
  }

  val SOME_DROPPED_COL = keywordOf(Tag.ContainerComponentID)
  val SOME_DUMMY_DATE_COL = keywordOf(Tag.ContentDate)
  val SOME_DUMMY_TIME_COL = keywordOf(Tag.ContentTime)
  val SOME_DUMMY_DATE_TIME_COL = keywordOf(Tag.AttributeModificationDateTime)
  val SOME_DUMMY_STRING_COL = keywordOf(Tag.AnnotationGroupLabel)
  val SOME_ZERO_INT_COL = keywordOf(Tag.PregnancyStatus)
  val SOME_ZERO_STRING_COL = keywordOf(Tag.ConceptualVolumeDescription)
  val SOME_EMPTY_STRING_COL = keywordOf(Tag.ConceptualVolumeDescription)

  describe("Spark") {
    it("Deidentify DICOM DataFrame") {
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
      assertThrows[IllegalArgumentException]{
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
  }
}
