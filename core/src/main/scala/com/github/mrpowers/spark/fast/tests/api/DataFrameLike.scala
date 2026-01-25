package com.github.mrpowers.spark.fast.tests.api

/**
 * Type class for DataFrame-like operations that work across different DataFrame implementations (Spark, Snowpark, etc.)
 *
 * @tparam F
 *   the DataFrame type
 * @tparam R
 *   the row type
 */
trait DataFrameLike[F, R] {

  /** Returns the schema of the DataFrame */
  def schema(df: F): SchemaLike

  /** Collects all rows from the DataFrame */
  def collect(df: F): Array[R]

  /** Returns the column names */
  def columns(df: F): Array[String]

  /** Returns the number of rows (may be expensive for large DataFrames) */
  def count(df: F): Long

  /** Selects columns by name and returns a new DataFrame */
  def select(df: F, columns: Seq[String]): F

  /** Sorts the DataFrame by all columns */
  def sort(df: F): F

  /** Returns the data types of columns as (columnName, typeName) pairs */
  def dtypes(df: F): Array[(String, String)]
}

object DataFrameLike {

  /** Summoner method to get the DataFrameLike instance for type F */
  def apply[F, R](implicit ev: DataFrameLike[F, R]): DataFrameLike[F, R] = ev

  /** Extension methods for any type F that has a DataFrameLike instance */
  implicit class DataFrameLikeOps[F, R](private val df: F) {
    def schema(implicit ev: DataFrameLike[F, R]): SchemaLike              = ev.schema(df)
    def collect(implicit ev: DataFrameLike[F, R]): Array[R]               = ev.collect(df)
    def columns(implicit ev: DataFrameLike[F, R]): Array[String]          = ev.columns(df)
    def count(implicit ev: DataFrameLike[F, R]): Long                     = ev.count(df)
    def select(cols: Seq[String])(implicit ev: DataFrameLike[F, R]): F    = ev.select(df, cols)
    def sort(implicit ev: DataFrameLike[F, R]): F                         = ev.sort(df)
    def dtypes(implicit ev: DataFrameLike[F, R]): Array[(String, String)] = ev.dtypes(df)
  }
}
