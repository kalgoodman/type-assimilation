package com.typeassimilation.implicits

import javafx.collections.ObservableList
import collection.JavaConversions._
import EnhancedSeq.Implicits._
import scalafx.collections.ObservableBuffer

class EnhancedObservableBuffer[T](observableBuffer: ObservableBuffer[T]) {
  def replaceWith(newList: Seq[T]): Unit = {
    val currentAsSeq = observableBuffer.toSeq
    val itemsToRemove = currentAsSeq -- newList
    val itemsToAdd = newList -- currentAsSeq
    observableBuffer --= itemsToRemove
    observableBuffer ++= itemsToAdd
  }
}

object EnhancedObservableBuffer {
  object Implicits {
    implicit def observableListToEnhancedObservableBuffer[T](observableBuffer: ObservableBuffer[T]) = new EnhancedObservableBuffer(observableBuffer)
  }
}
