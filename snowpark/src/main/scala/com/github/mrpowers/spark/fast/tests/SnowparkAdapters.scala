package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.api._

import scala.language.implicitConversions

/**
 * Adapter to convert Snowpark Row to RowLike.
 */
class SnowparkRowAdapter(private[tests] val row: com.snowflake.snowpark.Row) extends RowLike {

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

  override def getBinary(index: Int): Array[Byte] = row.getBinary(index)

  override def equals(obj: Any): Boolean = obj match {
    case other: SnowparkRowAdapter => row.equals(other.row)
    case other: RowLike            => super.equals(other)
    case _                         => false
  }

  override def hashCode(): Int = row.hashCode()

  override def toString: String = row.toString
}

object SnowparkRowAdapter {
  def apply(row: com.snowflake.snowpark.Row): SnowparkRowAdapter = new SnowparkRowAdapter(row)

  implicit def snowparkRowToRowLike(row: com.snowflake.snowpark.Row): RowLike = new SnowparkRowAdapter(row)
}

/**
 * Adapter to convert Snowpark StructField to FieldLike.
 */
class SnowparkFieldAdapter(field: com.snowflake.snowpark.types.StructField) extends FieldLike {

  override def name: String = field.name

  override def dataType: DataTypeLike = SnowparkDataTypeAdapter.convert(field.dataType)

  override def nullable: Boolean = field.nullable

  override def metadata: Map[String, Any] = Map.empty // Snowpark doesn't have field metadata
}

object SnowparkFieldAdapter {
  def apply(field: com.snowflake.snowpark.types.StructField): SnowparkFieldAdapter =
    new SnowparkFieldAdapter(field)
}

/**
 * Adapter to convert Snowpark StructType to SchemaLike.
 */
class SnowparkSchemaAdapter(schema: com.snowflake.snowpark.types.StructType) extends SchemaLike {

  override def fields: Seq[FieldLike] = schema.toSeq.map(SnowparkFieldAdapter.apply)
}

object SnowparkSchemaAdapter {
  def apply(schema: com.snowflake.snowpark.types.StructType): SnowparkSchemaAdapter =
    new SnowparkSchemaAdapter(schema)
}

/**
 * Converter for Snowpark DataType to DataTypeLike.
 */
object SnowparkDataTypeAdapter {
  import com.snowflake.snowpark.types._

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
    case a: ArrayType   => ArrayTypeLike(convert(a.elementType), true)
    case m: MapType     => MapTypeLike(convert(m.keyType), convert(m.valueType), true)
    case s: StructType  => StructTypeLike(s.toSeq.map(f => SnowparkFieldAdapter(f)))
    case other          => UnknownTypeLike(other.toString)
  }
}

/**
 * DataFrameLike instance for Snowpark DataFrame.
 */
object SnowparkDataFrameLike extends DataFrameLike[com.snowflake.snowpark.DataFrame, RowLike] {
  import com.snowflake.snowpark.functions.col

  override def schema(df: com.snowflake.snowpark.DataFrame): SchemaLike =
    SnowparkSchemaAdapter(df.schema)

  override def collect(df: com.snowflake.snowpark.DataFrame): Array[RowLike] =
    df.collect().map(SnowparkRowAdapter.apply)

  override def columns(df: com.snowflake.snowpark.DataFrame): Array[String] =
    df.schema.names.toArray

  override def count(df: com.snowflake.snowpark.DataFrame): Long = df.count()

  override def select(df: com.snowflake.snowpark.DataFrame, columns: Seq[String]): com.snowflake.snowpark.DataFrame =
    df.select(columns)

  override def sort(df: com.snowflake.snowpark.DataFrame): com.snowflake.snowpark.DataFrame = {
    val sortCols = df.schema.names.map(col)
    df.sort(sortCols)
  }

  override def dtypes(df: com.snowflake.snowpark.DataFrame): Array[(String, String)] =
    df.schema.map(f => (f.name, f.dataType.toString)).toArray

  implicit val instance: DataFrameLike[com.snowflake.snowpark.DataFrame, RowLike] = this
}
