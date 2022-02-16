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
package ai.kaiko.dicom.json

import org.dcm4che3.data
import org.dcm4che3.json.JSONWriter

import javax.json.Json
import javax.json.JsonArray
import javax.json.JsonObject
import javax.json.JsonStructure

import collection.JavaConverters._

object DicomJson {
  def attrs2jsonobject(attrs: data.Attributes): JsonObject = {
    val baos = new java.io.ByteArrayOutputStream
    val generator = Json
      .createGeneratorFactory(Map.empty.asJava)
      .createGenerator(baos)
    val writer = new JSONWriter(generator)
    writer.write(attrs)
    generator.flush

    val bais = new java.io.ByteArrayInputStream(baos.toByteArray)
    val reader = javax.json.Json.createReader(bais)
    reader.readObject
  }

  def seq2jsonarray(seq: data.Sequence): JsonArray = {
    val jab = javax.json.Json.createArrayBuilder()
    seq.asScala.toList.foreach(attr => {
      jab.add(DicomJson.attrs2jsonobject(attr))
    })
    jab.build
  }

  def json2string(json: JsonStructure): String = {
    val sw = new java.io.StringWriter
    val jwf =
      javax.json.Json.createWriterFactory(Map.empty.asJava)
    val jw = jwf.createWriter(sw)
    jw.write(json)
    jw.close
    sw.toString
  }
}
