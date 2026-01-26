package com.github.mrpowers.spark.fast.tests.api

/**
 * Abstract representation of a Row that works across different DataFrame implementations (Spark, Snowpark, etc.)
 */
trait RowLike extends Serializable {

  /** Total number of columns in this row */
  def length: Int

  /** Total number of columns in this row. Alias of [[length]] */
  def size: Int = length

  /** Returns the value at the given index */
  def get(index: Int): Any

  /** Returns the value at the given index. Alias of [[get]] */
  def apply(index: Int): Any = get(index)

  /** Returns true if the value at the given index is null */
  def isNullAt(index: Int): Boolean = get(index) == null

  /** Converts this row to a Seq */
  def toSeq: Seq[Any]

  /** Returns the value at the given index as a Boolean */
  def getBoolean(index: Int): Boolean

  /** Returns the value at the given index as a Byte */
  def getByte(index: Int): Byte

  /** Returns the value at the given index as a Short */
  def getShort(index: Int): Short

  /** Returns the value at the given index as an Int */
  def getInt(index: Int): Int

  /** Returns the value at the given index as a Long */
  def getLong(index: Int): Long

  /** Returns the value at the given index as a Float */
  def getFloat(index: Int): Float

  /** Returns the value at the given index as a Double */
  def getDouble(index: Int): Double

  /** Returns the value at the given index as a String */
  def getString(index: Int): String

  /** Returns the value at the given index as a BigDecimal */
  def getDecimal(index: Int): java.math.BigDecimal

  /** Returns the value at the given index as a Date */
  def getDate(index: Int): java.sql.Date

  /** Returns the value at the given index as a Timestamp */
  def getTimestamp(index: Int): java.sql.Timestamp

  /** Returns the value at the given index as a byte array */
  def getBinary(index: Int): Array[Byte]
}
