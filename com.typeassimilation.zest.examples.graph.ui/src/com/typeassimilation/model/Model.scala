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

case class DataType(val filePath: FilePath.Absolute, name: String, description: Option[String], assimilationReferences: Seq[AssimilationReference]) {
  def isPreset = false
  def reference = DataTypeReference(filePath)
  lazy val directAssimilation = new Assimilation(filePath, None, None, Seq(reference), isPreset) { override val isDirect = true }
}

case class DataTypeReference(filePath: FilePath)
case class AssimilationReference(name: Option[String], description: Option[String], filePath: FilePath, minimumOccurences: Option[Int], maximumOccurences: Option[Int], multipleOccurenceName: Option[String]) {
  override def toString = name.getOrElse("<ANONYMOUS>") + multipleOccurenceName.map(mon => s"($mon)").getOrElse("") + description.map(d => s" '$d'").getOrElse("") + s" -> $filePath {${minimumOccurences.getOrElse("*")},${maximumOccurences.getOrElse("*")}}"  
}
case class AbsoluteAssimilationReference(dataType: DataType, assimilationReference: AssimilationReference) {
  def assimilationOption(implicit model: Model) = model.assimilationOption(assimilationReference.filePath.toAbsoluteUsingBase(dataType.filePath.parent.get))
  def assimilation(implicit model: Model) = assimilationOption.get
  override def toString = s"${dataType.name} (${dataType.filePath}) -> ${assimilationReference}" 
}

object Preset {
  val RootFilePath = "/$".asAbsolute
  object DataType {
    private val presetDataTypes = mutable.Set.empty[DataType]
    private def presetPath(name: String) = RootFilePath + name.asRelative
    private def presetDataType(name: String, description: String) = {
      val dt = new DataType(presetPath(name), name, Some(description), Seq()) { override val isPreset = true }
      presetDataTypes += dt
      dt
    }
    val Identifier = presetDataType("IDENTIFIER", "The preset identifier type.")
    val DateTime = presetDataType("DATETIME", "The preset date/time type.")
    val Date = presetDataType("DATE", "The preset date type.")
    val Time = presetDataType("TIME", "The preset time type.")
    val IntegralNumber = presetDataType("INTEGER", "The preset integral number type (any whole number).")
    val DecimalNumber = presetDataType("DECIMAL", "The preset decimal number type (any number that isn't integral).")
    val Boolean = presetDataType("BOOLEAN", "The preset boolean type.")
    val Code2 = presetDataType("CODE2", "The preset coded value of character length 2 type.")
    val Code3 = presetDataType("CODE3", "The preset coded value of character length 3 type.")
    val TextBlock = presetDataType("TEXTBLOCK", "The preset block of text type.")
    val ShortName = presetDataType("SHORTNAME", "The preset short name type.")
    val LongName = presetDataType("LONGNAME", "The preset long name type.")
    private def assimilationReference(name: String, description: String, filePath: FilePath) = new AssimilationReference(Some(name), Some(description), filePath, Some(1), Some(1), None)
    val Money = {
      val name = "MONEY"
      val dt = new DataType(presetPath(name), name, Some("The preset monetary amount type."), Seq(
        assimilationReference("Amount", "The value of the monetary amount - i.e. The number without the currency.", DecimalNumber.filePath),
        assimilationReference("Currency", "The 3 character currency code (ISO 4217) for the monetary amount.", Code3.filePath)))
      presetDataTypes += dt
      dt
    }
    lazy val All = presetDataTypes.toSet
  }
}

class Assimilation(val filePath: FilePath.Absolute, val name: Option[String], val description: Option[String], initialDataTypes: Seq[DataTypeReference], val isPreset: Boolean) {
  val dataTypeReferences = ObservableBuffer(initialDataTypes)
  val isDirect = false
  def isSingular = dataTypeReferences.size == 1
}

case class JoinedAssimilationPath[T](assimilationPaths: Set[AssimilationPath[T]]) {
  private def delegateTipAssimilationPath = assimilationPaths.head
  def tip = delegateTipAssimilationPath.tip
  def tipAssimilationReference = delegateTipAssimilationPath.tipAssimilationReference
  def tipDataTypeOption = delegateTipAssimilationPath.tipDataTypeOption
  def tipDataType = delegateTipAssimilationPath.tipDataType
  def tipAssimilation(implicit model: Model) = delegateTipAssimilationPath.tipAssimilation
  def tipDataTypes(implicit model: Model) = delegateTipAssimilationPath.tipDataTypes
  def singleTipDataType(implicit model: Model) = delegateTipAssimilationPath.singleTipDataType
  // These 2 probably need rework...
  def tipName = delegateTipAssimilationPath.tipName
  def tipDescription = delegateTipAssimilationPath.tipDescription

  def +(assimilationPath: AssimilationPath[T]) = if (assimilationPath.tip == tip) copy(assimilationPaths = assimilationPaths + assimilationPath) else throw new IllegalStateException(s"Tips are inconsistent.")
  def +(joinedAssimilationPath: JoinedAssimilationPath[_]) = if (joinedAssimilationPath.tip == tip) copy(assimilationPaths = assimilationPaths ++ joinedAssimilationPath.asInstanceOf[JoinedAssimilationPath[T]].assimilationPaths) else throw new IllegalStateException(s"Tips are inconsistent.")
  def +(assimilationReference: AssimilationReference): JoinedAssimilationPath[AssimilationReference] = copy(assimilationPaths = assimilationPaths.map(_ + assimilationReference))
  def +(dataType: DataType): JoinedAssimilationPath[DataType] = copy(assimilationPaths = assimilationPaths.map(_ + dataType))
  private def toJoinedAssimilationPaths[T](aps: Set[AssimilationPath[T]]) = aps.groupBy(_.tip).values.map(JoinedAssimilationPath[T](_)).toSet
  def parents = toJoinedAssimilationPaths[Nothing](assimilationPaths.flatMap(_.parent))
  def assimilationReferenceParents = toJoinedAssimilationPaths(assimilationPaths.flatMap(_.assimilationReferenceParent).toSet)
  def dataTypeParents = toJoinedAssimilationPaths(assimilationPaths.flatMap(_.dataTypeParent).toSet)
  def parentToChildMappings[P](parentAssimilationPath: JoinedAssimilationPath[P]): Set[(AssimilationPath[P], Set[AssimilationPath[T]])] = parentAssimilationPath.assimilationPaths.map(pap => pap -> assimilationPaths.filter(_.isChildOf(pap)))
  def relativeTo(parentAssimilationPath: JoinedAssimilationPath[_]): RelativeJoinedAssimilationPath[T] = {
    if (!isChildOf(parentAssimilationPath)) throw new IllegalStateException(s"The path $parentAssimilationPath is not a root path of $this.")
    RelativeJoinedAssimilationPath(JoinedAssimilationPath(
      {
        parentToChildMappings(parentAssimilationPath).flatMap {
          case (p, cs) => cs.map(_.relativeTo(p))
        }
      }.map(_.assimilationPath)), parentAssimilationPath.tipDataTypeOption.isDefined)
  }
  def commonAssimilationReferences = {
    val (longest, rest) = assimilationPaths.map(_.assimilationReferences).toSeq.sortBy(-_.size).splitAt(1)
    longest.head.flatMap { ar =>
      if (rest.foldLeft(true)((present, rar) => present && rar.contains(ar))) Some(ar)
      else None
    }
  }
  def isChildOf(parentAssimilationPath: JoinedAssimilationPath[_]) = parentToChildMappings(parentAssimilationPath).foldLeft(0)(_ + _._2.size) == assimilationPaths.size
  override def toString = if (assimilationPaths.size == 1) assimilationPaths.head.toString else s"{\n\t${assimilationPaths.mkString("\n\t")}\n}"
}
case class RelativeJoinedAssimilationPath[T](assimilationPath: JoinedAssimilationPath[T], startOnAssimilationReference: Boolean)

object JoinedAssimilationPath {
  def apply[T](assimilationPath: AssimilationPath[T]): JoinedAssimilationPath[T] = JoinedAssimilationPath[T](Set(assimilationPath))
  def apply(dataType: DataType): JoinedAssimilationPath[DataType] = apply(AssimilationPath(dataType))
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
  def +(dataType: DataType): AssimilationPath[DataType] = if (tipDataTypeOption.isDefined) throw new IllegalStateException(s"The tip is not currently an Assimilation reference (it is a dataType - ${tipDataType})") else copy(tipDataTypeOption = Some(dataType))
  def parent = tip match {
    case Left(dataType) => if (assimilationReferences.isEmpty) None else Some(copy(tipDataTypeOption = None))
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
    case Left(dataType) => assimilationReferenceParent.flatMap(_.tipAssimilationReference.assimilationReference.name).getOrElse(dataType.name)
    case Right(assimilationReference) => assimilationReference.assimilationReference.name.getOrElse(assimilationReference.dataType.name + " Type")
  }
  def tipDescription = tip match {
    case Left(dataType) => dataType.description
    case Right(assimilationReference) => assimilationReference.assimilationReference.description
  }
  def tipDataTypes(implicit model: Model) = tipAssimilation.dataTypeReferences.flatMap(dtr => model.dataTypeOption(dtr.filePath.toAbsoluteUsingBase(tipAssimilation.filePath.parent.get)))
  def singleTipDataType(implicit model: Model) = tipDataTypes.head
  def relativeTo(parentAssimilationPath: AssimilationPath[_]): RelativeAssimilationPath[T] =
    if (!isChildOf(parentAssimilationPath)) throw new IllegalStateException(s"The path $parentAssimilationPath is not a root path of $this.")
    else RelativeAssimilationPath(AssimilationPath(assimilationReferences.drop(parentAssimilationPath.assimilationReferences.size), tipDataTypeOption), parentAssimilationPath.tipDataTypeOption.isDefined)
  def isChildOf(assimilationPath: AssimilationPath[_]) = assimilationReferences.startsWith(assimilationPath.assimilationReferences) && (!assimilationPath.tipDataTypeOption.isDefined || assimilationReferences(assimilationPath.assimilationReferences.size).dataType == assimilationPath.tipDataTypeOption.get)
  override def toString = {
    def descriptor(dataType: DataType) = s"${dataType.name} (${dataType.filePath})"
    (assimilationReferences.map(ar => s"${descriptor(ar.dataType)} -> ${ar.assimilationReference.name.getOrElse("<ANONYMOUS>")} " + (if(ar.assimilationReference.filePath.stringValue.endsWith(".type.xml")) "=" else s"(${ar.assimilationReference.filePath.toAbsoluteUsingBase(ar.dataType.filePath.parent.get)}) ")) ++ tipDataTypeOption.map(descriptor)).mkString("=> ")
  }
}
case class RelativeAssimilationPath[T](assimilationPath: AssimilationPath[T], startOnAssimilationReference: Boolean)

object AssimilationPath {
  def apply(rootDataType: DataType): AssimilationPath[DataType] = AssimilationPath[DataType](Seq(), Some(rootDataType))
}

object Model {
  object Implicits {
    implicit def dataTypeToEnhancedDataType(dataType: DataType) = new EnhancedDataType(dataType)
    implicit def assimilationToEnhancedAssimilation(assimilation: Assimilation) = new EnhancedAssimilation(assimilation)
  }
}

class Model(definedDataTypes: Set[DataType], definedAssimilations: Set[Assimilation], val defaultMinimumOccurences: Int = 0, val defaultMaximumOccurences: Option[Int] = Some(1)) {
  import Model.Implicits._
  implicit val _ = this
  val dataTypes = definedDataTypes ++ Preset.DataType.All
  def dataTypeOption(filePath: FilePath.Absolute) = dataTypes.find(_.filePath == filePath)
  val assimilations = definedAssimilations ++ dataTypes.map(_.directAssimilation) 
  def assimilationOption(filePath: FilePath.Absolute) = assimilations.find(_.filePath == filePath)
  def rootDataTypes = dataTypes.filter(dt => dt.referencedSelfAssimilations.isEmpty && !dt.isPreset)
}

class EnhancedDataType(dataType: DataType) {
  def assimilations(implicit model: Model) = dataType.assimilationReferences.map(ar => model.assimilationOption(ar.filePath.toAbsoluteUsingBase(dataType.filePath.parent.get)) match {
    case Some(a) => a
    case None => throw new IllegalStateException(s"Reference made from data type ${dataType.filePath} to assimilation ${ar.filePath} which doesn't exist.")
  })
  def definedSelfAssimilations(implicit model: Model) = model.assimilations.filter(a => !a.isDirect && a.dataTypeReferences.map(_.filePath.toAbsoluteUsingBase(a.filePath.parent.get)).contains(dataType.filePath))
  def referencedSelfAssimilations(implicit model: Model) = {
    val referenceAssimilationFilePaths = model.dataTypes.flatMap(dt => dt.assimilationReferences.map(_.filePath.toAbsoluteUsingBase(dt.filePath.parent.get)))
    (definedSelfAssimilations + dataType.directAssimilation).flatMap { a => if (referenceAssimilationFilePaths.contains(a.filePath)) Some(a) else None }
  }
}

class EnhancedAssimilation(assimilation: Assimilation) {
  def dataTypes(implicit model: Model) = assimilation.dataTypeReferences.map(dtr => model.dataTypeOption(dtr.filePath.toAbsoluteUsingBase(assimilation.filePath.parent.get)) match {
    case Some(dt) => dt
    case None => throw new IllegalStateException(s"Reference made from assimilation ${assimilation.filePath} to data type ${dtr.filePath} which doesn't exist.")
  })
  def singleDataType(implicit model: Model) = if (assimilation.dataTypeReferences.size == 1) dataTypes.head else throw new IllegalStateException(s"The code assumes a single data type but this assimilation references multiple data types: ${assimilation.dataTypeReferences.map(_.filePath).mkString(",")}")
}

