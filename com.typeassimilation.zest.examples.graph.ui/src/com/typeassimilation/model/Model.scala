package com.typeassimilation.model

import scalafx.beans.property.StringProperty
import javafx.collections.ObservableList
import collection.JavaConversions._
import javafx.collections.FXCollections
import com.typeassimilation.implicits.EnhancedObservableBuffer.Implicits._
import com.typeassimilation.implicits.EnhancedSeq.Implicits._
import scala.collection.mutable
import scalafx.collections.ObservableBuffer
import scalafx.collections.ObservableSet
import scala.collection.mutable
import scalafx.beans.Observable
import java.io.File
import com.typeassimilation.implicits.EnhancedSeq
import com.typeassimilation.implicits.EnhancedObservableBuffer

sealed trait ModelPart[T <: ModelPart[T]] {
  def filePath: FilePath.Absolute
  protected def updateNonNameProperties(t: T) = {}
  def updateTo(t: T): Boolean = if (t.filePath == filePath) {
    updateNonNameProperties(t)
    true
  } else false
}

object ModelPart {
  def updateModelPartsObservableBuffer[T <: ModelPart[T]](observableCollectionToUpdate: Iterable[T] with Observable, newObservableCollection: Iterable[T] with Observable): Unit = {
    val partsToAdd = mutable.ListBuffer.empty[T]
    newObservableCollection.foreach { newDt =>
      observableCollectionToUpdate.find(_.filePath == newDt.filePath) match {
        case Some(oldDt) => oldDt.updateTo(newDt)
        case None => partsToAdd += newDt
      }
    }
    val partsToRemove = observableCollectionToUpdate.toSeq -- newObservableCollection.toSeq
    observableCollectionToUpdate.removeAll(partsToRemove)
    observableCollectionToUpdate.addAll(partsToAdd)
  } 
}

class DataType(val filePath: FilePath.Absolute, initialName: String, initialDescription: String, initialAssimilations: Seq[AssimilationReference]) extends ModelPart[DataType] {
  val name = StringProperty(initialName)
  val description = StringProperty(initialDescription)
  val assimilationReferences = ObservableSet(initialAssimilations.toArray:_*)
  override def updateNonNameProperties(dataType: DataType): Unit = {
    description.value = dataType.description()
  } 
}

case class DataTypeReference(filePath: FilePath)
case class AssimilationReference(name: Option[String], filePath: FilePath)

class Assimilation(val filePath: FilePath.Absolute, initialDataTypes: Seq[DataTypeReference]) extends ModelPart[Assimilation] {
  val dataTypeReferences = ObservableBuffer(initialDataTypes)
  override def updateNonNameProperties(assimilation: Assimilation): Unit = {
    dataTypeReferences.replaceWith(assimilation.dataTypeReferences)
  }
}

class Model(initialTypes: Traversable[DataType], initialAssimilations: Traversable[Assimilation]) {
  val dataTypes = ObservableSet(initialTypes.toArray:_*)
  def dataTypeOption(filePath: FilePath.Absolute) = dataTypes.find(_.filePath == filePath)
  val assimilations = ObservableSet(initialAssimilations.toArray:_*)
  def assimilationOption(filePath: FilePath.Absolute) = assimilations.find(_.filePath == filePath)
  def updateTo(newModel: Model): Unit = {
    ModelPart.updateModelPartsObservableBuffer(dataTypes, newModel.dataTypes)
    ModelPart.updateModelPartsObservableBuffer(assimilations, newModel.assimilations)
  }
}

object Model {
  object Implicits {
    def dataTypeToEnhancedDataType(dataType: DataType) = new EnhancedDataType(dataType)
    def assimilationToEnhancedAssimilation(assimilation: Assimilation) = new EnhancedAssimilation(assimilation)
  }
}

class EnhancedDataType(dataType: DataType) {
  def assimilations(implicit model: Model) = dataType.assimilationReferences.map(ar => model.assimilationOption(ar.filePath.toAbsoluteUsingBase(dataType.filePath.parent.get)) match {
    case Some(a) => a
    case None => throw new IllegalStateException(s"Reference made from data type ${dataType.filePath} to assimilation ${ar.filePath} which doesn't exist.")
  })
}

class EnhancedAssimilation(assimilation: Assimilation) {
  def dataTypes(implicit model: Model) = assimilation.dataTypeReferences.map(dtr => model.dataTypeOption(dtr.filePath.toAbsoluteUsingBase(assimilation.filePath.parent.get)) match {
    case Some(dt) => dt
    case None => throw new IllegalStateException(s"Reference made from assimilation ${assimilation.filePath} to data type ${dtr.filePath} which doesn't exist.")
  })
}