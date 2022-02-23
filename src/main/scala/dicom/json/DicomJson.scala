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

import ai.kaiko.dicom.DicomStandardDictionary
import org.dcm4che3.data
import org.dcm4che3.json.JSONWriter

import javax.json.Json
import javax.json.JsonArray
import javax.json.JsonObject
import javax.json.JsonStructure
import javax.json.JsonValue

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
    val reader = Json.createReader(bais)
    reader.readObject
  }

  def seq2jsonarray(seq: data.Sequence): JsonArray = {
    val jab = Json.createArrayBuilder()
    seq.asScala.toList.foreach(attr => {
      jab.add(DicomJson.attrs2jsonobject(attr))
    })
    jab.build
  }

  def json2string(json: JsonStructure): String = {
    val sw = new java.io.StringWriter
    val jwf = Json.createWriterFactory(Map.empty.asJava)
    val jw = jwf.createWriter(sw)
    jw.write(json)
    jw.close
    sw.toString
  }

  def deepRenameJsonKeys(
      jsonArr: JsonArray,
      renameF: String => String
  ): JsonArray = {
    val outBuilder = Json.createArrayBuilder
    jsonArr.forEach(jsonVal => {
      val outVal = jsonVal.getValueType match {
        case JsonValue.ValueType.OBJECT =>
          deepRenameJsonKeys(jsonVal.asJsonObject, renameF)
        case JsonValue.ValueType.ARRAY =>
          deepRenameJsonKeys(jsonVal.asJsonArray, renameF)
        case _ => jsonVal
      }
      outBuilder.add(outVal)
    })
    outBuilder.build
  }

  def deepRenameJsonKeys(
      jsonObj: JsonObject,
      renameF: String => String
  ): JsonObject = {
    val outBuilder = javax.json.Json.createObjectBuilder
    jsonObj.entrySet.asScala.foreach(entry => {
      val outKey = renameF(entry.getKey)
      val outValue = entry.getValue.getValueType match {
        case JsonValue.ValueType.OBJECT =>
          deepRenameJsonKeys(entry.getValue.asJsonObject, renameF)
        case JsonValue.ValueType.ARRAY =>
          deepRenameJsonKeys(entry.getValue.asJsonArray, renameF)
        case _ => entry.getValue
      }
      outBuilder.add(outKey, outValue)
    })
    outBuilder.build
  }

  def deepRenameJsonKeyTagToKeyword(
      jsonStruct: JsonStructure
  ): JsonStructure = {
    def renameTagToKeywordOrFallback(key: String) =
      scala.util
        .Try(Integer.parseInt(key, 16))
        .map(intTag =>
          DicomStandardDictionary.tagMap
            .get(intTag)
            .map(elemDict => elemDict.keyword)
        )
        .toOption
        .flatten
        .getOrElse(key)

    jsonStruct match {
      case jsonObj: JsonObject =>
        deepRenameJsonKeys(jsonObj, renameTagToKeywordOrFallback(_))
      case jsonArr: JsonArray =>
        deepRenameJsonKeys(jsonArr, renameTagToKeywordOrFallback(_))
    }
  }
}
