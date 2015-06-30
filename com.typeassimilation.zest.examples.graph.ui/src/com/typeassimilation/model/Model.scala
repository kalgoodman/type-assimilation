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
import FilePath.Implicits._

sealed trait ModelPart[T <: ModelPart[T]] {
  def filePath: FilePath.Absolute
  protected def updateNonNameProperties(t: T) = {}
  def updateTo(t: T): Boolean = if (t.filePath == filePath) {
    updateNonNameProperties(t)
    true
  } else false
  
  override def hashCode = filePath.hashCode
  override def equals(other: Any) = other match {
    case mp : ModelPart[_] => filePath == mp.filePath
    case _ => false
  }
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
  val assimilationReferences = ObservableBuffer(initialAssimilations.toArray:_*)
  override def updateNonNameProperties(dataType: DataType): Unit = {
    description.value = dataType.description()
  } 
  
  val isPreset = false
}

case class DataTypeReference(filePath: FilePath)
case class AssimilationReference(name: Option[String], description: Option[String], filePath: FilePath, minimumOccurences: Option[Int], maximumOccurences: Option[Int], multipleOccurenceName: Option[String])
case class AbsoluteAssimilationReference(dataType: DataType, assimilationReference: AssimilationReference) {
    def assimilationOption(implicit model: Model) = model.assimilationOption(assimilationReference.filePath.toAbsoluteUsingBase(dataType.filePath.parent.get))
    def assimilation(implicit model: Model) = assimilationOption.get
}
  
object Preset {
  val RootFilePath = "/$".asAbsolute
  object DataType {
    private val presetDataTypes = mutable.Set.empty[DataType]
    private def presetDataType(name: String, description: String) = {
      val dt = new DataType(RootFilePath + name.asRelative, name, description, Seq()) { override val isPreset = true }
      presetDataTypes += dt
      dt
    }
    val Identifier = presetDataType("IDENTIFIER","The preset identifier type.")
    val DateTime = presetDataType("DATETIME", "The preset date/time type.")
    val Date = presetDataType("DATE", "The preset date type.")
    val Money = presetDataType("MONEY", "The preset money type (implies amount and currency).")
    val IntegralNumber = presetDataType("INTEGER", "The preset integral number type (any whole number).")
    val DecimalNumber = presetDataType("DECIMAL", "The preset decimal number type (any number that isn't integral).")
    val Boolean = presetDataType("BOOLEAN", "The preset boolean type.")
    val CharacterString = presetDataType("STRING", "The preset character string type.")
    lazy val All = presetDataTypes.toSet 
  }
  object Assimilation {
    private val presetAssimilations = mutable.Set.empty[Assimilation]
    private def presetAssimilation(dataType: DataType) = {
      val a = new Assimilation(dataType.filePath, Seq(DataTypeReference(dataType.filePath))) { override val isPreset = true }
      presetAssimilations += a
      a
    }
    val Identifier = presetAssimilation(DataType.Identifier)
    val DateTime = presetAssimilation(DataType.DateTime)
    val Date = presetAssimilation(DataType.Date)
    val Money = presetAssimilation(DataType.Money)
    val IntegralNumber = presetAssimilation(DataType.IntegralNumber)
    val DecimalNumber = presetAssimilation(DataType.DecimalNumber)
    val Boolean = presetAssimilation(DataType.Boolean)
    val CharacterString = presetAssimilation(DataType.CharacterString)
    lazy val All = presetAssimilations.toSet
  } 
}

class Assimilation(val filePath: FilePath.Absolute, initialDataTypes: Seq[DataTypeReference]) extends ModelPart[Assimilation] {
  val dataTypeReferences = ObservableBuffer(initialDataTypes)
  override def updateNonNameProperties(assimilation: Assimilation): Unit = {
    dataTypeReferences.replaceWith(assimilation.dataTypeReferences)
  }
  val isPreset = false
  def isSingular = dataTypeReferences.size == 1
}

case class AssimilationPath[T](assimilationReferences: Seq[AbsoluteAssimilationReference], tipDataTypeOption: Option[DataType]) {
  def tipDataType = tipDataTypeOption.get
  def tipAssimilationReference = assimilationReferences.last
  def tipAssimilation(implicit model: Model) = tipAssimilationReference.assimilation
  def tip: Either[DataType, AbsoluteAssimilationReference] = tipDataTypeOption match {
    case None => Right(tipAssimilationReference)
    case Some(dataType) => Left(dataType)
  }
  def +(assimilationReference: AssimilationReference): AssimilationPath[AssimilationReference] = copy(assimilationReferences = assimilationReferences :+ AbsoluteAssimilationReference(tipDataTypeOption.getOrElse(throw new IllegalStateException(s"The tip is not currently a DataType (it is an assimilation reference - $tipAssimilationReference)")), assimilationReference), tipDataTypeOption = None)
  def +(dataType: DataType): AssimilationPath[DataType] = if(tipDataTypeOption.isDefined) throw new IllegalStateException(s"The tip is not currently an Assimilation reference (it is a dataType - ${tipDataType})") else copy(tipDataTypeOption = Some(dataType))
  def parent = tip match {
    case Left(dataType) => if(assimilationReferences.isEmpty) None else Some(copy(tipDataTypeOption = None))
    case Right(assimilationReference) => Some(copy(assimilationReferences = assimilationReferences.dropRight(1), tipDataTypeOption = Some(assimilationReference.dataType)))
  }
  def assimilationReferenceParent: Option[AssimilationPath[AssimilationReference]] = parent.flatMap { ap =>
    ap.tip match {
      case Left(dataType) => None
      case Right(assimilationReference) => Some(ap.asInstanceOf[AssimilationPath[AssimilationReference]])
    }  
  }
  def dataTypeParent: Option[AssimilationPath[DataType]] = parent.flatMap { ap =>
    ap.tip match {
      case Left(dataType) => Some(ap.asInstanceOf[AssimilationPath[DataType]])
      case Right(assimilationReference) => None
    }  
  }
  def tipName = tip match {
    case Left(dataType) => assimilationReferenceParent.flatMap(_.tipAssimilationReference.assimilationReference.name).getOrElse(dataType.name())
    case Right(assimilationReference) => assimilationReference.assimilationReference.name.getOrElse(assimilationReference.dataType.name() + " Type")
  }
  def tipDescription = tip match {
    case Left(dataType) => if (dataType.description() == "") None else Some(dataType.description()) 
    case Right(assimilationReference) => assimilationReference.assimilationReference.description
  }
  def tipDataTypes(implicit model: Model) = tipAssimilation.dataTypeReferences.flatMap(dtr => model.dataTypeOption(dtr.filePath.toAbsoluteUsingBase(tipAssimilation.filePath.parent.get)))
  def singleTipDataType(implicit model: Model) = tipDataTypes.head
}

object AssimilationPath {
  def apply(rootDataType: DataType): AssimilationPath[DataType] = AssimilationPath[DataType](Seq(), Some(rootDataType))
}

object Model {
  object Implicits {
    implicit def dataTypeToEnhancedDataType(dataType: DataType) = new EnhancedDataType(dataType)
    implicit def assimilationToEnhancedAssimilation(assimilation: Assimilation) = new EnhancedAssimilation(assimilation)
  }
}

class Model(initialTypes: Traversable[DataType], initialAssimilations: Traversable[Assimilation], val defaultMinimumOccurences: Int = 0, val defaultMaximumOccurences: Option[Int] = Some(1)) {
  import Model.Implicits._
  implicit val _ = this
  val dataTypes = ObservableSet(initialTypes.toArray:_*) ++ Preset.DataType.All
  def dataTypeOption(filePath: FilePath.Absolute) = dataTypes.find(_.filePath == filePath)
  val assimilations = ObservableSet(initialAssimilations.toArray:_*) ++ Preset.Assimilation.All
  def assimilationOption(filePath: FilePath.Absolute) = assimilations.find(_.filePath == filePath)
  def rootDataTypes = dataTypes.filter(_.selfAssimilations.isEmpty)
  def updateTo(newModel: Model): Unit = {
    ModelPart.updateModelPartsObservableBuffer(dataTypes, newModel.dataTypes)
    ModelPart.updateModelPartsObservableBuffer(assimilations, newModel.assimilations)
  }
}

class EnhancedDataType(dataType: DataType) {
  def assimilations(implicit model: Model) = dataType.assimilationReferences.map(ar => model.assimilationOption(ar.filePath.toAbsoluteUsingBase(dataType.filePath.parent.get)) match {
    case Some(a) => a
    case None => throw new IllegalStateException(s"Reference made from data type ${dataType.filePath} to assimilation ${ar.filePath} which doesn't exist.")
  })
  def selfAssimilations(implicit model: Model) = model.assimilations.filter(a => a.dataTypeReferences.map(_.filePath.toAbsoluteUsingBase(a.filePath.parent.get)).contains(dataType.filePath))
}

class EnhancedAssimilation(assimilation: Assimilation) {
  def dataTypes(implicit model: Model) = assimilation.dataTypeReferences.map(dtr => model.dataTypeOption(dtr.filePath.toAbsoluteUsingBase(assimilation.filePath.parent.get)) match {
    case Some(dt) => dt
    case None => throw new IllegalStateException(s"Reference made from assimilation ${assimilation.filePath} to data type ${dtr.filePath} which doesn't exist.")
  })
  def singleDataType(implicit model: Model) = if (assimilation.dataTypeReferences.size == 1) dataTypes.head else throw new IllegalStateException(s"The code assumes a single data type but this assimilation references multiple data types: ${assimilation.dataTypeReferences.map(_.filePath).mkString(",")}")
}

