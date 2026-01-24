package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.api._
import org.apache.spark.sql.{DataFrame, Row}
import com.github.mrpowers.spark.fast.tests.DataframeDiffOutputFormat.DataframeDiffOutputFormat

/**
 * Trait for comparing Spark DataFrames in tests. Delegates to core DataFrameLikeComparer for small DataFrame comparisons.
 */
trait DataFrameComparer extends DatasetComparer {

  /**
   * Raises an error unless `actualDF` and `expectedDF` are equal. Delegates to core DataFrameLikeComparer.
   */
  def assertSmallDataFrameEquality(
      actualDF: DataFrame,
      expectedDF: DataFrame,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false,
      ignoreMetadata: Boolean = true,
      truncate: Int = 500,
      outputFormat: DataframeDiffOutputFormat = DataframeDiffOutputFormat.SideBySide
  ): Unit = {
    try {
      assertDataFrameLikeEquality[DataFrame](
        actualDF,
        expectedDF,
        ignoreNullable,
        ignoreColumnNames,
        orderedComparison,
        ignoreColumnOrder,
        ignoreMetadata,
        truncate,
        equals = (r1: RowLike, r2: RowLike) => r1.equals(r2),
        outputFormat
      )(sparkDataFrameLike)
    } catch {
      case e: ContentMismatch =>
        throw DatasetContentMismatch(e.getMessage)
      case e: SchemaMismatch =>
        throw SchemaComparer.DatasetSchemaMismatch(e.getMessage)
    }
  }

  /**
   * Raises an error unless `actualDF` and `expectedDF` are equal. Uses RDD-based approach for large DataFrames.
   */
  def assertLargeDataFrameEquality(
      actualDF: DataFrame,
      expectedDF: DataFrame,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false,
      ignoreMetadata: Boolean = true
  ): Unit = {
    assertLargeDatasetEquality(
      actualDF,
      expectedDF,
      ignoreNullable = ignoreNullable,
      ignoreColumnNames = ignoreColumnNames,
      orderedComparison = orderedComparison,
      ignoreColumnOrder = ignoreColumnOrder,
      ignoreMetadata = ignoreMetadata
    )
  }

  /**
   * Raises an error unless `actualDF` and `expectedDF` are approximately equal. Delegates to core DataFrameLikeComparer.
   */
  def assertApproximateSmallDataFrameEquality(
      actualDF: DataFrame,
      expectedDF: DataFrame,
      precision: Double,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false,
      ignoreMetadata: Boolean = true,
      truncate: Int = 500,
      outputFormat: DataframeDiffOutputFormat = DataframeDiffOutputFormat.SideBySide
  ): Unit = {
    try {
      assertApproximateDataFrameLikeEquality[DataFrame](
        actualDF,
        expectedDF,
        precision,
        ignoreNullable,
        ignoreColumnNames,
        orderedComparison,
        ignoreColumnOrder,
        ignoreMetadata,
        truncate,
        outputFormat
      )(sparkDataFrameLike)
    } catch {
      case e: ContentMismatch =>
        throw DatasetContentMismatch(e.getMessage)
      case e: SchemaMismatch =>
        throw SchemaComparer.DatasetSchemaMismatch(e.getMessage)
    }
  }

  /**
   * Raises an error unless `actualDF` and `expectedDF` are approximately equal. Uses RDD-based approach for large DataFrames.
   */
  def assertApproximateLargeDataFrameEquality(
      actualDF: DataFrame,
      expectedDF: DataFrame,
      precision: Double,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      orderedComparison: Boolean = true,
      ignoreColumnOrder: Boolean = false,
      ignoreMetadata: Boolean = true
  ): Unit = {
    assertLargeDatasetEquality[Row](
      actualDF,
      expectedDF,
      equals = RowComparer.areRowsEqual(_, _, precision),
      ignoreNullable,
      ignoreColumnNames,
      orderedComparison,
      ignoreColumnOrder,
      ignoreMetadata
    )
  }
}
