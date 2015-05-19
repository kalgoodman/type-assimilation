package com.typeassimilation.implicits

case class EnhancedSeq[T](seq: Seq[T]) {
  def --(traversable: Traversable[T]): Seq[T] = {
    val traversableSet = traversable.toSet
    seq.flatMap(element => if(traversableSet.contains(element)) None else Some(element))
  }
}

object EnhancedSeq {
  object Implicits {
    implicit def seqToEnhancedSeq[T](seq: Seq[T]) = EnhancedSeq(seq)
  }
}