package com.github.mrpowers.spark.fast.tests.api

/**
 * Common exception types for DataFrame comparison. These are in the api package to avoid Spark/Snowpark dependencies.
 */
case class ContentMismatch(smth: String)     extends Exception(smth)
case class SchemaMismatch(smth: String)      extends Exception(smth)
case class CountMismatch(smth: String)       extends Exception(smth)
case class ColumnMismatch(smth: String)      extends Exception(smth)
case class ColumnOrderMismatch(smth: String) extends Exception(smth)
