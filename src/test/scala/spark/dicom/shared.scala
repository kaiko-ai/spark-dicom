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

import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.CancelAfterFailure
import org.scalatest.Suite
import org.scalatest.funspec.AnyFunSpec

trait WithSpark extends Suite with BeforeAndAfterAll {
  val spark = {
    val spark = SparkSession.builder.master("local").getOrCreate
    spark.sparkContext.setLogLevel(Level.ERROR.toString())
    spark
  }

  override def afterAll() {
    spark.stop
  }
}

trait WithTestLogger {
  val TEST_LOGGER_NAME = "test"
  val testLogger = {
    val logger = LogManager.getLogger("test");
    logger.setLevel(Level.DEBUG)
    logger
  }
}

abstract class SparkTest
    extends AnyFunSpec
    with WithSpark
    with WithTestLogger
    // Spark test can take some time, cancel after failure to end faster
    with CancelAfterFailure
