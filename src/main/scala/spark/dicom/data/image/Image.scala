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
package spark.dicom.data.image

import org.apache.spark.sql.functions._
import org.dcm4che3.imageio.plugins.dcm.DicomImageReader
import org.dcm4che3.imageio.plugins.dcm.DicomImageReaderSpi
import org.dcm4che3.io.DicomInputStream

import java.io.ByteArrayInputStream
import java.io._
import javax.imageio._

case class Image(
    data: Array[Byte],
    height: Option[Int] = Option.empty,
    width: Option[Int] = Option.empty
)

object Image {

  def image(
      binary: Array[Byte],
      outputImageFormat: String = "png",
      dimensions: Boolean = false
  ): Array[Image] = {
    val reader = new DicomImageReader(new DicomImageReaderSpi)
    scala.util.Try
      .apply({
        val dicomInputStream =
          new DicomInputStream(new ByteArrayInputStream(binary))
        reader.setInput(dicomInputStream)
        Seq.range(0, reader.getNumImages(true)).map { index =>
          {
            val image = reader.read(index, reader.getDefaultReadParam)
            val imageByteStream = new ByteArrayOutputStream
            ImageIO.write(image, outputImageFormat, imageByteStream)
            val ba = imageByteStream.toByteArray
            if (dimensions) {
              val img = ImageIO.read(new ByteArrayInputStream(ba))
              Image(ba, Some(img.getHeight), Some(img.getWidth))
            } else {
              Image(ba)
            }
          }
        }
      })
      .getOrElse(Seq.empty)
      .toArray
  }

  def toImage(outputImageFormat: String = "png") = udf { (b: Array[Byte]) =>
    image(b, outputImageFormat)
  }
}
