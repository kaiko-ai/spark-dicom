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
package ai.kaiko
package spark.dicom.data.meta

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object DeepEmpty {

  private def fn(name: String, prefix: String*) =
    prefix.foldRight(name)((a, b) => a + "." + b)

  def isDeepEmpty(df: DataFrame): Seq[Column] = isDeepEmpty(df.schema.fields)

  def isDeepEmpty(fields: Seq[StructField], prefix: String*): Seq[Column] =
    fields.map(f => {
      val name = fn(f.name, prefix: _*)
      val c: Column = col(name)
      when(
        c.isNull || (f.dataType match {
          case StringType => trim(c) === lit("")
          case ArrayType(_, _) =>
            size(c) === lit(0) // TODO: deep empty for arrays
          case StructType(s) =>
            isDeepEmpty(s, (prefix :+ f.name): _*).reduce(_ + _) === lit(0)
          case _ => lit(false)
        }),
        lit(0)
      ).otherwise(lit(1)).alias(name)
    })

  // drop all deep empty columns
  def dropDeepEmpty(d: DataFrame) = {
    val ise = isDeepEmpty(d).map(sum)
    val colsEmpty = d.select(ise: _*).collect
    val colsToDrop = (0 until colsEmpty.head.length)
      .map(i => colsEmpty.head.getLong(i) == 0)
      .zip(d.columns)
      .filter { x => x._1 }
      .map(_._2)
    d.drop(colsToDrop: _*)
  }
}
