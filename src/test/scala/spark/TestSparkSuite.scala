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
package ai.kaiko.spark

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level

import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suites

import ai.kaiko.spark.dicom.deidentifier.TestDicomDeidentifier
import ai.kaiko.spark.dicom.TestDicomDataSource

trait WithSpark {
  var spark = {
    val spark = SparkSession.builder.master("local").getOrCreate
    spark.sparkContext.setLogLevel(Level.ERROR.toString())
    spark
  }
}

// Here we make sure to include all tests that require a sparkSession.
// We close the spark session after all tests in the suite have run.
class TestSparkSuite extends Suites(
  new TestDicomDeidentifier, 
  new TestDicomDataSource
) 
  with WithSpark 
  with BeforeAndAfterAll {

  override protected def afterAll(): Unit = {
    spark.stop
  }   
}
