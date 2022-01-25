package ai.kaiko
package spark.dicom.data.meta

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}

object DeepEmpty {

  private def fn(name: String, prefix: String*) = prefix.foldRight(name)((a, b) => a + "." + b)

  def isDeepEmpty(df: DataFrame) = isDeepEmpty(df.schema.fields)

  def isDeepEmpty(fields: Seq[StructField], prefix: String*): Seq[Column] = fields.map(f => {
    val name = fn(f.name, prefix: _*)
    val c: Column = col(name)
    when(c.isNull || (f.dataType match {
      case StringType => trim(c) === lit("")
      case ArrayType(_, _) => size(c) === lit(0) // TODO: deep empty for arrays
      case StructType(s) => isDeepEmpty(s, (prefix :+ f.name): _*).reduce(_ + _) === lit(0)
      case _ => lit(false)
    }), lit(0)).otherwise(lit(1)).alias(name)
  })

  // drop all deep empty columns
  def dropDeepEmpty(d: DataFrame) = {
    val ise = isDeepEmpty(d).map(sum)
    val colsEmpty = d.select(ise: _*).collect
    val colsToDrop = (0 until colsEmpty.head.length)
      .map(i => colsEmpty.head.getLong(i) == 0)
      .zip(d.columns).filter { x => x._1 }.map(_._2)
    d.drop(colsToDrop: _*)
  }
}
