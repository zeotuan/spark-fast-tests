package com.github.mrpowers.spark.fast.tests

import scala.collection.{GenIterable, SeqLike}

object SeqLikeExtensions {

  implicit class SeqExtensions[T, Repr <: SeqLike[T, Repr]](val seq1: SeqLike[T, Repr]) extends AnyVal {
    def approximateSameElements(seq2: GenIterable[T], equals: (T, T) => Boolean): Boolean = (seq1, seq2) match {
      case (i1: IndexedSeq[_], i2: IndexedSeq[_]) =>
        val len = i1.length
        len == i2.length && {
          var i = 0
          while (i < len && equals(i1(i).asInstanceOf[T], i2(i).asInstanceOf[T])) i += 1
          i == len
        }
      case _ => seq1.sameElements(seq2)
    }
  }
}
