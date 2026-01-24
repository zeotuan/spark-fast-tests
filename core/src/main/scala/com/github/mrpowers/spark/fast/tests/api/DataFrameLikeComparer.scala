package com.github.mrpowers.spark.fast.tests.api

import com.github.mrpowers.spark.fast.tests.DataframeDiffOutputFormat.DataframeDiffOutputFormat
import com.github.mrpowers.spark.fast.tests.DataframeDiffOutputFormat
import com.github.mrpowers.spark.fast.tests.SchemaDiffOutputFormat.SchemaDiffOutputFormat
import com.github.mrpowers.spark.fast.tests.SchemaDiffOutputFormat
import com.github.mrpowers.spark.fast.tests.api.SeqLikeExtensions.SeqExtensions

/**
 * Generic DataFrame comparison trait that works with any DataFrame type via the DataFrameLike type class. This trait has no Spark dependencies and
 * can be used by both Spark and Snowpark modules.
 */
trait DataFrameLikeComparer {

  /**
   * Orders columns of df1 according to df2's column order. Generic version that works with any DataFrame type.
   */
  def orderColumnsGeneric[F](df1: F, df2: F)(implicit ev: DataFrameLike[F]): F = {
    ev.select(df1, ev.columns(df2).toSeq)
  }

  /**
   * Sorts the DataFrame by all columns. Generic version that works with any DataFrame type.
   */
  def defaultSortGeneric[F](df: F)(implicit ev: DataFrameLike[F]): F = ev.sort(df)

  /**
   * Raises an error unless `actualDF` and `expectedDF` are equal. Generic version that works with any DataFrame type via DataFrameLike type class.
   */
  def assertDataFrameLikeEquality[F](
      actualDF: F,
      expectedDF: F,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false,
      ignoreMetadata: Boolean = true,
      truncate: Int = 500,
      equals: (RowLike, RowLike) => Boolean = (o1: RowLike, o2: RowLike) => o1.equals(o2),
      outputFormat: DataframeDiffOutputFormat = DataframeDiffOutputFormat.SideBySide,
      schemaDiffOutputFormat: SchemaDiffOutputFormat = SchemaDiffOutputFormat.Table
  )(implicit ev: DataFrameLike[F]): Unit = {
    // Check schema equality using generic SchemaLike comparison
    SchemaLikeComparer.assertSchemaEqual(
      ev.schema(actualDF),
      ev.schema(expectedDF),
      ignoreNullable,
      ignoreColumnNames,
      ignoreColumnOrder,
      ignoreMetadata,
      schemaDiffOutputFormat
    )
    val actual = if (ignoreColumnOrder) orderColumnsGeneric(actualDF, expectedDF) else actualDF
    assertDataFrameLikeContentEquality(actual, expectedDF, orderedComparison, truncate, equals, outputFormat)
  }

  /**
   * Compares content of two DataFrames. Generic version that works with any DataFrame type.
   */
  def assertDataFrameLikeContentEquality[F](
      actualDF: F,
      expectedDF: F,
      orderedComparison: Boolean,
      truncate: Int,
      equals: (RowLike, RowLike) => Boolean,
      outputFormat: DataframeDiffOutputFormat
  )(implicit ev: DataFrameLike[F]): Unit = {
    val (actual, expected) = if (orderedComparison) {
      (actualDF, expectedDF)
    } else {
      (defaultSortGeneric(actualDF), defaultSortGeneric(expectedDF))
    }

    val actualRows   = ev.collect(actual).toSeq
    val expectedRows = ev.collect(expected).toSeq

    if (!actualRows.approximateSameElements(expectedRows, equals)) {
      val msg = "Diffs\n" ++ ProductLikeUtil.showProductDiff(
        ev.columns(actual),
        actualRows,
        expectedRows,
        truncate,
        outputFormat = outputFormat
      )
      throw ContentMismatch(msg)
    }
  }

  /**
   * Raises an error unless `actualDF` and `expectedDF` are approximately equal. Generic version that works with any DataFrame type. Uses tolerance
   * for numeric comparisons.
   */
  def assertApproximateDataFrameLikeEquality[F](
      actualDF: F,
      expectedDF: F,
      precision: Double,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false,
      ignoreMetadata: Boolean = true,
      truncate: Int = 500,
      outputFormat: DataframeDiffOutputFormat = DataframeDiffOutputFormat.SideBySide
  )(implicit ev: DataFrameLike[F]): Unit = {
    val equalsWithPrecision = (r1: RowLike, r2: RowLike) => {
      r1.equals(r2) || RowLikeComparer.areRowsEqual(r1, r2, precision)
    }
    assertDataFrameLikeEquality(
      actualDF,
      expectedDF,
      ignoreNullable,
      ignoreColumnNames,
      orderedComparison,
      ignoreColumnOrder,
      ignoreMetadata,
      truncate,
      equalsWithPrecision,
      outputFormat
    )
  }
}

object DataFrameLikeComparer extends DataFrameLikeComparer
