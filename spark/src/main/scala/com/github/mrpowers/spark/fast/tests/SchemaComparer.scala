package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.api.ProductLikeUtil.showProductDiff
import com.github.mrpowers.spark.fast.tests.SchemaDiffOutputFormat.SchemaDiffOutputFormat
import com.github.mrpowers.spark.fast.tests.api._
import com.github.mrpowers.spark.fast.tests.comparer.schema.FieldComparison.{areFieldsEqual, buildComparison, treeSchemaMismatchMessage}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._

object SchemaComparer {
  case class DatasetSchemaMismatch(smth: String) extends Exception(smth)

  // ============ Primary implementation using SchemaLike abstractions ============
  // All comparison logic is here - works with any DataFrame implementation

  private def schemaLikeMismatchMessage(actualSchema: SchemaLike, expectedSchema: SchemaLike): String = {
    showProductDiff(
      Array("Actual Schema", "Expected Schema"),
      actualSchema.fields.map(f => s"${f.name}: ${f.dataType.typeName} (nullable=${f.nullable})"),
      expectedSchema.fields.map(f => s"${f.name}: ${f.dataType.typeName} (nullable=${f.nullable})"),
      truncate = 200
    )
  }

  // Spark-specific mismatch message that uses StructField.toString format (for backward compatibility)
  private def sparkSchemaMismatchMessage(actualSchema: StructType, expectedSchema: StructType): String = {
    showProductDiff(
      Array("Actual Schema", "Expected Schema"),
      actualSchema.fields,
      expectedSchema.fields,
      truncate = 200
    )
  }

  /**
   * Asserts that two SchemaLike schemas are equal. This is the primary implementation that works with any DataFrame implementation.
   */
  def assertSchemaEqual(
      actualSchema: SchemaLike,
      expectedSchema: SchemaLike,
      ignoreNullable: Boolean,
      ignoreColumnNames: Boolean,
      ignoreColumnOrder: Boolean,
      ignoreMetadata: Boolean,
      outputFormat: SchemaDiffOutputFormat
  ): Unit = {
    require((ignoreColumnNames, ignoreColumnOrder) != (true, true), "Cannot set both ignoreColumnNames and ignoreColumnOrder to true.")
    val diffTree = buildComparison(actualSchema, expectedSchema, ignoreColumnOrder)
    if (!areFieldsEqual(diffTree, ignoreNullable, ignoreColumnNames, ignoreMetadata)) {
      val diffString = outputFormat match {
        case SchemaDiffOutputFormat.Tree  => treeSchemaMismatchMessage(diffTree)
        case SchemaDiffOutputFormat.Table => schemaLikeMismatchMessage(actualSchema, expectedSchema)
      }
      throw DatasetSchemaMismatch(s"Diffs\n$diffString")
    }
  }

  /**
   * Asserts that two Spark StructType schemas are equal. Uses Spark-native output format for better compatibility.
   */
  def assertSparkSchemaEqual(
      actualSchema: StructType,
      expectedSchema: StructType,
      ignoreNullable: Boolean,
      ignoreColumnNames: Boolean,
      ignoreColumnOrder: Boolean,
      ignoreMetadata: Boolean,
      outputFormat: SchemaDiffOutputFormat
  ): Unit = {
    require((ignoreColumnNames, ignoreColumnOrder) != (true, true), "Cannot set both ignoreColumnNames and ignoreColumnOrder to true.")
    val actualSchemaLike   = convertSparkSchema(actualSchema)
    val expectedSchemaLike = convertSparkSchema(expectedSchema)
    val diffTree           = buildComparison(actualSchemaLike, expectedSchemaLike, ignoreColumnOrder)
    if (!areFieldsEqual(diffTree, ignoreNullable, ignoreColumnNames, ignoreMetadata)) {
      val diffString = outputFormat match {
        case SchemaDiffOutputFormat.Tree  => treeSchemaMismatchMessage(diffTree)
        case SchemaDiffOutputFormat.Table => sparkSchemaMismatchMessage(actualSchema, expectedSchema)
      }
      throw DatasetSchemaMismatch(s"Diffs\n$diffString")
    }
  }

  /**
   * Compares two SchemaLike schemas for equality. This is the primary implementation that works with any DataFrame implementation.
   */
  def equals(
      s1: SchemaLike,
      s2: SchemaLike,
      ignoreNullable: Boolean,
      ignoreColumnNames: Boolean,
      ignoreColumnOrder: Boolean,
      ignoreMetadata: Boolean
  ): Boolean = {
    if (s1.length != s2.length) {
      false
    } else {
      val zipStruct = if (ignoreColumnOrder) {
        s1.sortedByName zip s2.sortedByName
      } else {
        s1.fields zip s2.fields
      }
      zipStruct.forall { case (f1, f2) =>
        (f1.nullable == f2.nullable || ignoreNullable) &&
        (f1.name == f2.name || ignoreColumnNames) &&
        (f1.metadata == f2.metadata || ignoreMetadata) &&
        dataTypeEquals(f1.dataType, f2.dataType, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata)
      }
    }
  }

  /**
   * Compares two DataTypeLike types for equality.
   */
  def dataTypeEquals(
      dt1: DataTypeLike,
      dt2: DataTypeLike,
      ignoreNullable: Boolean,
      ignoreColumnNames: Boolean,
      ignoreColumnOrder: Boolean,
      ignoreMetadata: Boolean
  ): Boolean = {
    (dt1, dt2) match {
      case (st1: StructTypeLike, st2: StructTypeLike) =>
        equals(st1, st2, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata)
      case (ArrayTypeLike(vdt1, _), ArrayTypeLike(vdt2, _)) =>
        dataTypeEquals(vdt1, vdt2, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata)
      case (MapTypeLike(kdt1, vdt1, _), MapTypeLike(kdt2, vdt2, _)) =>
        dataTypeEquals(kdt1, kdt2, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata) &&
        dataTypeEquals(vdt1, vdt2, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata)
      case _ => dt1 == dt2
    }
  }

  // ============ Spark-specific convenience methods (delegate to generic) ============

  /**
   * Asserts that two Spark Dataset schemas are equal. Uses Spark-native output format for backward compatibility.
   */
  def assertDatasetSchemaEqual[T](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      ignoreColumnOrder: Boolean = true,
      ignoreMetadata: Boolean = true,
      outputFormat: SchemaDiffOutputFormat = SchemaDiffOutputFormat.Table
  ): Unit = {
    assertSparkSchemaEqual(
      actualDS.schema,
      expectedDS.schema,
      ignoreNullable,
      ignoreColumnNames,
      ignoreColumnOrder,
      ignoreMetadata,
      outputFormat
    )
  }

  /**
   * Asserts that two Spark StructType schemas are equal. Uses Spark-native output format for backward compatibility.
   */
  def assertSchemaEqual(
      actualSchema: StructType,
      expectedSchema: StructType,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      ignoreColumnOrder: Boolean = true,
      ignoreMetadata: Boolean = true,
      outputFormat: SchemaDiffOutputFormat = SchemaDiffOutputFormat.Table
  ): Unit = {
    assertSparkSchemaEqual(
      actualSchema,
      expectedSchema,
      ignoreNullable,
      ignoreColumnNames,
      ignoreColumnOrder,
      ignoreMetadata,
      outputFormat
    )
  }

  /**
   * Compares two Spark StructType schemas for equality. Convenience method that converts Spark types and delegates to generic implementation.
   */
  def equals(
      s1: StructType,
      s2: StructType,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      ignoreColumnOrder: Boolean = true,
      ignoreMetadata: Boolean = true
  ): Boolean = {
    equals(
      convertSparkSchema(s1),
      convertSparkSchema(s2),
      ignoreNullable,
      ignoreColumnNames,
      ignoreColumnOrder,
      ignoreMetadata
    )
  }

  // ============ Spark to SchemaLike conversion ============

  /**
   * Converts a Spark StructType to SchemaLike abstraction.
   */
  def convertSparkSchema(schema: StructType): SchemaLike = {
    StructTypeLike(schema.fields.map(convertSparkField).toSeq)
  }

  /**
   * Converts a Spark StructField to FieldLike abstraction.
   */
  def convertSparkField(field: StructField): FieldLike = {
    GenericFieldLike(
      name = field.name,
      dataType = convertSparkDataType(field.dataType),
      nullable = field.nullable,
      metadata = if (field.metadata.equals(Metadata.empty)) Map.empty else Map("_sparkMetadata" -> field.metadata)
    )
  }

  /**
   * Converts a Spark DataType to DataTypeLike abstraction.
   */
  def convertSparkDataType(dt: DataType): DataTypeLike = dt match {
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
    case a: ArrayType   => ArrayTypeLike(convertSparkDataType(a.elementType), a.containsNull)
    case m: MapType     => MapTypeLike(convertSparkDataType(m.keyType), convertSparkDataType(m.valueType), m.valueContainsNull)
    case s: StructType  => StructTypeLike(s.fields.map(convertSparkField).toSeq)
    case other          => UnknownTypeLike(other.typeName)
  }

  // ============ Legacy method aliases for backward compatibility ============

  /** @deprecated Use assertSchemaEqual instead */
  def assertSchemaLikeEqual(
      actualSchema: SchemaLike,
      expectedSchema: SchemaLike,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      ignoreColumnOrder: Boolean = true,
      ignoreMetadata: Boolean = true
  ): Unit = {
    assertSchemaEqual(
      actualSchema,
      expectedSchema,
      ignoreNullable,
      ignoreColumnNames,
      ignoreColumnOrder,
      ignoreMetadata,
      SchemaDiffOutputFormat.Table
    )
  }

  /** @deprecated Use equals instead */
  def schemaLikeEquals(
      s1: SchemaLike,
      s2: SchemaLike,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      ignoreColumnOrder: Boolean = true,
      ignoreMetadata: Boolean = true
  ): Boolean = {
    equals(s1, s2, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata)
  }

  /** @deprecated Use dataTypeEquals instead */
  def dataTypeLikeEquals(
      dt1: DataTypeLike,
      dt2: DataTypeLike,
      ignoreNullable: Boolean,
      ignoreColumnNames: Boolean,
      ignoreColumnOrder: Boolean,
      ignoreMetadata: Boolean
  ): Boolean = {
    dataTypeEquals(dt1, dt2, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata)
  }
}
