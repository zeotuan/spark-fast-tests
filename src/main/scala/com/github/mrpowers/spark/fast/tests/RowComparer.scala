package com.github.mrpowers.spark.fast.tests

import org.apache.commons.math3.util.Precision

import scala.math.abs
import org.apache.spark.sql.Row

object RowComparer {

  /** Approximate equality, based on equals from [[Row]] */
  def areRowsEqual(r1: Row, r2: Row, tol: Double = 0, strictType: Boolean = true): Boolean = {
    if (tol == 0 && strictType) {
      return r1 == r2
    }
    if (r1.length != r2.length) {
      return false
    }
    for (i <- 0 until r1.length) {
      if (r1.isNullAt(i) != r2.isNullAt(i)) {
        return false
      }
      if (!r1.isNullAt(i)) {
        val o1 = r1.get(i)
        val o2 = r2.get(i)
        val valid = o1 match {
          case b1: Array[Byte] =>
            (o2.isInstanceOf[Array[Byte]] || !strictType) && java.util.Arrays.equals(b1, o2.asInstanceOf[Array[Byte]])
          case f1: Float => (o2.isInstanceOf[Float] || !strictType) &&
            Precision.equalsIncludingNaN(f1, o2.asInstanceOf[Float], tol)
          case d1: Double =>
            (o2.isInstanceOf[Double] || !strictType) &&
              Precision.equalsIncludingNaN(d1, o2.asInstanceOf[Double], tol)
          case bd1: java.math.BigDecimal if o2.isInstanceOf[java.math.BigDecimal] || !strictType =>
            bd1.subtract(o2.asInstanceOf[java.math.BigDecimal]).abs().compareTo(new java.math.BigDecimal(tol)) == -1
          case _ => o1 == o2
        }
        if (!valid) {
          return false
        }
      }
    }
    true
  }
}
