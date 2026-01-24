package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.api._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col

/**
 * Adapter to convert Spark Row to RowLike
 */
class SparkRowAdapter(private[tests] val row: Row) extends RowLike {

  override def length: Int = row.length

  override def get(index: Int): Any = row.get(index)

  override def isNullAt(index: Int): Boolean = row.isNullAt(index)

  override def toSeq: Seq[Any] = row.toSeq

  override def getBoolean(index: Int): Boolean = row.getBoolean(index)

  override def getByte(index: Int): Byte = row.getByte(index)

  override def getShort(index: Int): Short = row.getShort(index)

  override def getInt(index: Int): Int = row.getInt(index)

  override def getLong(index: Int): Long = row.getLong(index)

  override def getFloat(index: Int): Float = row.getFloat(index)

  override def getDouble(index: Int): Double = row.getDouble(index)

  override def getString(index: Int): String = row.getString(index)

  override def getDecimal(index: Int): java.math.BigDecimal = row.getDecimal(index)

  override def getDate(index: Int): java.sql.Date = row.getDate(index)

  override def getTimestamp(index: Int): java.sql.Timestamp = row.getTimestamp(index)

  override def getBinary(index: Int): Array[Byte] = row.getAs[Array[Byte]](index)

  override def equals(obj: Any): Boolean = obj match {
    case other: SparkRowAdapter => row.equals(other.row)
    case other: RowLike         => super.equals(other)
    case _                      => false
  }

  override def hashCode(): Int = row.hashCode()

  override def toString: String = row.toString
}

object SparkRowAdapter {
  def apply(row: Row): SparkRowAdapter = new SparkRowAdapter(row)

  implicit def rowToRowLike(row: Row): RowLike = new SparkRowAdapter(row)
}

/**
 * Adapter to convert Spark StructField to FieldLike
 */
class SparkFieldAdapter(field: StructField) extends FieldLike {

  override def name: String = field.name

  override def dataType: DataTypeLike = SparkDataTypeAdapter.convert(field.dataType)

  override def nullable: Boolean = field.nullable

  override def metadata: Map[String, Any] = {
    // Convert Spark Metadata to Map
    val m = field.metadata
    if (m.equals(Metadata.empty)) {
      Map.empty
    } else {
      m.json.hashCode() match {
        case _ => Map("_sparkMetadata" -> m) // preserve original for comparison
      }
    }
  }
}

object SparkFieldAdapter {
  def apply(field: StructField): SparkFieldAdapter = new SparkFieldAdapter(field)
}

/**
 * Adapter to convert Spark StructType to SchemaLike
 */
class SparkSchemaAdapter(schema: StructType) extends SchemaLike {

  override def fields: Seq[FieldLike] = schema.fields.map(SparkFieldAdapter.apply).toSeq
}

object SparkSchemaAdapter {
  def apply(schema: StructType): SparkSchemaAdapter = new SparkSchemaAdapter(schema)
}

/**
 * Converter for Spark DataType to DataTypeLike
 */
object SparkDataTypeAdapter {

  def convert(dt: DataType): DataTypeLike = dt match {
    case StringType     => StringTypeLike
    case BooleanType    => BooleanTypeLike
    case ByteType       => ByteTypeLike
    case ShortType      => ShortTypeLike
    case IntegerType    => IntegerTypeLike
    case LongType       => LongTypeLike
    case FloatType      => FloatTypeLike
    case DoubleType     => DoubleTypeLike
    case BinaryType     => BinaryTypeLike
    case DateType       => DateTypeLike
    case TimestampType  => TimestampTypeLike
    case d: DecimalType => DecimalTypeLike(d.precision, d.scale)
    case a: ArrayType   => ArrayTypeLike(convert(a.elementType), a.containsNull)
    case m: MapType     => MapTypeLike(convert(m.keyType), convert(m.valueType), m.valueContainsNull)
    case s: StructType  => StructTypeLike(s.fields.map(f => SparkFieldAdapter(f)).toSeq)
    case other          => UnknownTypeLike(other.typeName)
  }
}

/**
 * DataFrameLike instance for Spark DataFrame
 */
object SparkDataFrameLike extends DataFrameLike[DataFrame] {

  override def schema(df: DataFrame): SchemaLike = SparkSchemaAdapter(df.schema)

  override def collect(df: DataFrame): Array[RowLike] =
    df.collect().map(SparkRowAdapter.apply)

  override def columns(df: DataFrame): Array[String] = df.columns

  override def count(df: DataFrame): Long = df.count()

  override def select(df: DataFrame, columns: Seq[String]): DataFrame =
    df.select(columns.map(col): _*)

  override def sort(df: DataFrame): DataFrame =
    df.sort(df.columns.map(col): _*)

  override def dtypes(df: DataFrame): Array[(String, String)] = df.dtypes

  // Provide implicit for type class usage
  implicit val instance: DataFrameLike[DataFrame] = this
}
