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

object RowLike {
  import scala.reflect.ClassTag

  implicit val rowLikeClassTag: ClassTag[RowLike] = ClassTag(classOf[RowLike])

  /** Creates a generic RowLike from a sequence of values */
  def fromSeq(values: Seq[Any]): RowLike = new GenericRowLike(values.toArray)

  /** Creates a generic RowLike from an array of values */
  def fromArray(values: Array[Any]): RowLike = new GenericRowLike(values)
}

/**
 * A generic implementation of RowLike backed by an array
 */
class GenericRowLike(private val values: Array[Any]) extends RowLike {

  override def length: Int = values.length

  override def get(index: Int): Any = values(index)

  override def toSeq: Seq[Any] = values.toSeq

  override def getBoolean(index: Int): Boolean = get(index).asInstanceOf[Boolean]

  override def getByte(index: Int): Byte = get(index) match {
    case b: Byte  => b
    case s: Short => s.toByte
    case i: Int   => i.toByte
    case l: Long  => l.toByte
    case other    => throw new ClassCastException(s"Cannot cast $other to Byte")
  }

  override def getShort(index: Int): Short = get(index) match {
    case b: Byte  => b.toShort
    case s: Short => s
    case i: Int   => i.toShort
    case l: Long  => l.toShort
    case other    => throw new ClassCastException(s"Cannot cast $other to Short")
  }

  override def getInt(index: Int): Int = get(index) match {
    case b: Byte  => b.toInt
    case s: Short => s.toInt
    case i: Int   => i
    case l: Long  => l.toInt
    case other    => throw new ClassCastException(s"Cannot cast $other to Int")
  }

  override def getLong(index: Int): Long = get(index) match {
    case b: Byte  => b.toLong
    case s: Short => s.toLong
    case i: Int   => i.toLong
    case l: Long  => l
    case other    => throw new ClassCastException(s"Cannot cast $other to Long")
  }

  override def getFloat(index: Int): Float = get(index) match {
    case f: Float  => f
    case d: Double => d.toFloat
    case b: Byte   => b.toFloat
    case s: Short  => s.toFloat
    case i: Int    => i.toFloat
    case l: Long   => l.toFloat
    case other     => throw new ClassCastException(s"Cannot cast $other to Float")
  }

  override def getDouble(index: Int): Double = get(index) match {
    case f: Float  => f.toDouble
    case d: Double => d
    case b: Byte   => b.toDouble
    case s: Short  => s.toDouble
    case i: Int    => i.toDouble
    case l: Long   => l.toDouble
    case other     => throw new ClassCastException(s"Cannot cast $other to Double")
  }

  override def getString(index: Int): String = get(index) match {
    case null      => null
    case s: String => s
    case other     => other.toString
  }

  override def getDecimal(index: Int): java.math.BigDecimal =
    get(index).asInstanceOf[java.math.BigDecimal]

  override def getDate(index: Int): java.sql.Date =
    get(index).asInstanceOf[java.sql.Date]

  override def getTimestamp(index: Int): java.sql.Timestamp =
    get(index).asInstanceOf[java.sql.Timestamp]

  override def getBinary(index: Int): Array[Byte] =
    get(index).asInstanceOf[Array[Byte]]

  override def equals(obj: Any): Boolean = obj match {
    case other: RowLike =>
      if (length != other.length) false
      else
        (0 until length).forall { i =>
          (get(i), other.get(i)) match {
            case (d1: Double, d2: Double) if d1.isNaN && d2.isNaN => true
            case (v1, v2)                                         => v1 == v2
          }
        }
    case _ => false
  }

  override def hashCode(): Int = {
    var h = scala.util.hashing.MurmurHash3.seqSeed
    var i = 0
    while (i < length) {
      h = scala.util.hashing.MurmurHash3.mix(h, get(i).##)
      i += 1
    }
    scala.util.hashing.MurmurHash3.finalizeHash(h, length)
  }

  override def toString: String = toSeq.mkString("Row[", ",", "]")
}
