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

case class DataType(val filePath: FilePath.Absolute, name: String, description: Option[String], assimilations: Seq[Assimilation], isOrientating: Boolean) {
  def absoluteAssimilations = assimilations.map(a => AbsoluteAssimilation(this, a))
  def selfAssimilations(implicit model: Model) = model.dataTypes.flatMap(_.absoluteAssimilations).filter(_.dataTypeReferences.map(_.filePath).contains(filePath))
  def isEffectivelyOrientating(implicit model: Model) = isOrientating || model.orientatingDataTypes.contains(this)
  def identifyingAssimilations = assimilations.filter(_.isIdentifying)
  def preset = false
  def primitive = false
}

sealed trait AssimilationStrength {
  def representation: String
}
object AssimilationStrength {
  case object Reference extends AssimilationStrength { val representation = "REFERENCE" }
  case object Weak extends AssimilationStrength { val representation = "WEAK" }
  case object Strong extends AssimilationStrength { val representation = "STRONG" }
  def apply(s: String): AssimilationStrength = s match {
    case Reference.representation => Reference
    case Weak.representation => Weak
    case Strong.representation => Strong
    case _ => throw new IllegalArgumentException(s"There is no such strength as '$s'.")
  }
  implicit object Ordering extends Ordering[AssimilationStrength] {
    private val strengthRank = Map[AssimilationStrength, Int](
        Reference -> 0,
        Weak -> 1,
        Strong -> 2
      )
    def compare(x: AssimilationStrength, y: AssimilationStrength): Int = strengthRank(x).compareTo(strengthRank(y))
  }
}
sealed trait DataTypeReferenceLike {
  def filePath: FilePath
  def strength: Option[AssimilationStrength]
  def effectiveStrength(absoluteAssimilation: AbsoluteAssimilation)(implicit model: Model) = model.effectiveStrength(absoluteAssimilation, this)
  def toAbsolute(dataType: DataType): AbsoluteDataTypeReference
  def toAbsolute(absoluteAssimilation: AbsoluteAssimilation): AbsoluteDataTypeReference = toAbsolute(absoluteAssimilation.dataType)
}
case class DataTypeReference(filePath: FilePath, strength: Option[AssimilationStrength] = None) extends DataTypeReferenceLike {
  def toAbsolute(dataType: DataType) = AbsoluteDataTypeReference(filePath.toAbsoluteUsingBase(dataType.filePath.parent.get), strength)
}
case class AbsoluteDataTypeReference(filePath: FilePath.Absolute, strength: Option[AssimilationStrength] = None) extends DataTypeReferenceLike {
  def toAbsolute(dataType: DataType) = this
}
case class Assimilation(name: Option[String], description: Option[String], isIdentifying: Boolean, dataTypeReferences: Seq[DataTypeReference], minimumOccurences: Option[Int], maximumOccurences: Option[Int], multipleOccurenceName: Option[String]) {
  def absoluteDataTypeReferences(dataType: DataType) = dataTypeReferences.map(dtr => AbsoluteDataTypeReference(dtr.filePath.toAbsoluteUsingBase(dataType.filePath.parent.get), dtr.strength))
  override def toString = name.getOrElse("<ANONYMOUS>") + multipleOccurenceName.map(mon => s"($mon)").getOrElse("") + description.map(d => s" '$d'").getOrElse("") + s" -> [${dataTypeReferences.map(dtr => dtr.filePath).mkString(", ")}] {${minimumOccurences.getOrElse("*")},${maximumOccurences.getOrElse("*")}}"
}
case class AbsoluteAssimilation(dataType: DataType, assimilation: Assimilation) {
  def dataTypeReferences = assimilation.absoluteDataTypeReferences(dataType)
  def dataTypes(implicit model: Model) = dataTypeReferences.flatMap(dtr => model.dataTypeOption(dtr.filePath))
  override def toString = s"${dataType.name} (${dataType.filePath}) -> ${assimilation}"
}

object Preset {
  val RootFilePath = "/$".asAbsolute
  object DataType {
    private val presetDataTypes = mutable.Set.empty[DataType]
    private def presetPath(name: String) = RootFilePath + name.asRelative
    private def presetDataType(name: String, description: String) = {
      val dt = new DataType(presetPath(name), name, Some(description), Seq(), false) {
        override val preset = true
        override val primitive = true
      }
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
    private def assimilation(name: String, description: String, filePath: FilePath) = new Assimilation(Some(name), Some(description), false, Seq(DataTypeReference(filePath)), Some(1), Some(1), None)
    val Money = {
      val name = "MONEY"
      val dt = new DataType(presetPath(name), name, Some("The preset monetary amount type."), Seq(
        assimilation("Amount", "The value of the monetary amount - i.e. The number without the currency.", DecimalNumber.filePath),
        assimilation("Currency", "The 3 character currency code (ISO 4217) for the monetary amount.", Code3.filePath)), false) {
        override val preset = true
      }
      presetDataTypes += dt
      dt
    }
    lazy val All = presetDataTypes.toSet
  }
}

case class JoinedAssimilationPath[T](assimilationPaths: Set[AssimilationPath[T]]) {
  private def delegateTipAssimilationPath = assimilationPaths.head
  def tip = delegateTipAssimilationPath.tip
  def tipAssimilation = delegateTipAssimilationPath.tipAssimilation
  def tipAssimilationOption = delegateTipAssimilationPath.tipAssimilationOption
  def tipDataTypeOption = delegateTipAssimilationPath.tipDataTypeOption
  def tipDataType = delegateTipAssimilationPath.tipDataType
  def tipDataTypes(implicit model: Model) = delegateTipAssimilationPath.tipDataTypes
  def withSingleTipDataType(implicit model: Model) = copy(assimilationPaths = assimilationPaths.map(_.withSingleTipDataType))
  def singleTipDataType(implicit model: Model) = delegateTipAssimilationPath.singleTipDataType
  def isReferenceToOrientatingDataType(implicit model: Model) = delegateTipAssimilationPath.isReferenceToOrientatingDataType
  // These 2 probably need rework...
  def tipName = delegateTipAssimilationPath.tipName
  def tipDescription = delegateTipAssimilationPath.tipDescription
  def asDataType = this.asInstanceOf[JoinedAssimilationPath[DataType]]
  def asAssimilation = this.asInstanceOf[JoinedAssimilationPath[Assimilation]]
  
  def +(assimilationPath: AssimilationPath[T]) = if (assimilationPath.tip == tip) copy(assimilationPaths = assimilationPaths + assimilationPath) else throw new IllegalStateException(s"Tips are inconsistent.")
  def +(joinedAssimilationPath: JoinedAssimilationPath[_]) = if (joinedAssimilationPath.tip == tip) copy(assimilationPaths = assimilationPaths ++ joinedAssimilationPath.asInstanceOf[JoinedAssimilationPath[T]].assimilationPaths) else throw new IllegalStateException(s"Tips are inconsistent.")
  def +(assimilation: Assimilation): JoinedAssimilationPath[Assimilation] = copy(assimilationPaths = assimilationPaths.map(_ + assimilation))
  def +(dataType: DataType): JoinedAssimilationPath[DataType] = copy(assimilationPaths = assimilationPaths.map(_ + dataType))
  private def toJoinedAssimilationPaths[T](aps: Set[AssimilationPath[T]]) = aps.groupBy(_.tip).values.map(JoinedAssimilationPath[T](_)).toSet
  def parents = toJoinedAssimilationPaths[Nothing](assimilationPaths.flatMap(_.parent))
  def assimilationParents = toJoinedAssimilationPaths(assimilationPaths.flatMap(_.assimilationParent).toSet)
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
  lazy val commonAssimilations = {
    val (longest, rest) = assimilationPaths.map(_.assimilations).toSeq.sortBy(-_.size).splitAt(1)
    longest.head.flatMap { a =>
      if (rest.foldLeft(true)((present, rar) => present && rar.contains(a))) Some(a)
      else None
    }
  }
  lazy val commonLength = (commonAssimilations ++ tipDataTypeOption).size
  def isChildOf(parentAssimilationPath: JoinedAssimilationPath[_]) = parentToChildMappings(parentAssimilationPath).foldLeft(0)(_ + _._2.size) == assimilationPaths.size
  override def toString = if (assimilationPaths.size == 1) assimilationPaths.head.toString else s"{\n\t${assimilationPaths.mkString("\n\t")}\n}"
}
case class RelativeJoinedAssimilationPath[T](assimilationPath: JoinedAssimilationPath[T], startOnAssimilation: Boolean)

object JoinedAssimilationPath {
  def apply[T](assimilationPath: AssimilationPath[T]): JoinedAssimilationPath[T] = JoinedAssimilationPath[T](Set(assimilationPath))
  def apply(dataType: DataType): JoinedAssimilationPath[DataType] = apply(AssimilationPath(dataType))
}

case class AssimilationPath[T](parent: Option[AssimilationPath[Nothing]], tip: Either[DataType, AbsoluteAssimilation]) {
  lazy val assimilations: Seq[AbsoluteAssimilation] = {
    def recurse(assimilationPath: AssimilationPath[_]): Seq[AbsoluteAssimilation] = {
      assimilationPath.parent match {
        case None => assimilationPath.tipAssimilationOption.toSeq
        case Some(parent) => recurse(parent) ++ assimilationPath.tipAssimilationOption 
      }
    }
    recurse(this)
  } 
  lazy val tipDataTypeOption: Option[DataType] = tip match {
    case Left(dataType) => Some(dataType)
    case Right(assimilation) => None
  }  
  lazy val tipDataType = tipDataTypeOption.get
  lazy val tipAssimilation = assimilations.last
  lazy val tipAssimilationOption = tip match {
    case Left(dataType) => None
    case Right(assimilation) => Some(assimilation)
  }
  def +(assimilation: Assimilation): AssimilationPath[Assimilation] = tip match {
    case Right(assimilation) => throw new IllegalStateException(s"The tip is not currently a DataType (it is an assimilation reference - $tipAssimilation)")
    case Left(dataType) => AssimilationPath(Some(this.asInstanceOf[AssimilationPath[Nothing]]), Right(AbsoluteAssimilation(dataType, assimilation)))
  }
  def +(dataType: DataType): AssimilationPath[DataType] = tip match {
    case Left(dataType) => throw new IllegalStateException(s"The tip is not currently an Assimilation reference (it is a dataType - $dataType)")
    case Right(assimilation) => AssimilationPath(Some(this.asInstanceOf[AssimilationPath[Nothing]]), Left(dataType))
  }
  def assimilationParent: Option[AssimilationPath[Assimilation]] = parent.flatMap { ap =>
    ap.tip match {
      case Left(dataType) => None
      case Right(assimilation) => Some(ap.asInstanceOf[AssimilationPath[Assimilation]])
    }
  }
  def dataTypeParent: Option[AssimilationPath[DataType]] = parent.flatMap { ap =>
    ap.tip match {
      case Left(dataType) => Some(ap.asInstanceOf[AssimilationPath[DataType]])
      case Right(assimilation) => None
    }
  }
  def tipName = tip match {
    case Left(dataType) => assimilationParent.flatMap(_.tipAssimilation.assimilation.name).getOrElse(dataType.name)
    case Right(assimilation) => assimilation.assimilation.name.getOrElse(assimilation.dataType.name + " Type")
  }
  def tipDescription = tip match {
    case Left(dataType) => dataType.description
    case Right(assimilation) => assimilation.assimilation.description
  }
  def tipDataTypes(implicit model: Model) = tipAssimilation.dataTypes
  def withSingleTipDataType(implicit model: Model) = this + singleTipDataType
  def singleTipDataType(implicit model: Model) = tipDataTypes.head
  def relativeTo(parentAssimilationPath: AssimilationPath[_]): RelativeAssimilationPath[T] =
    if (!isChildOf(parentAssimilationPath)) throw new IllegalStateException(s"The path $parentAssimilationPath is not a root path of $this.")
    else {
      def pathSeq(ap: AssimilationPath[_]): Seq[AssimilationPath[_]] = if (ap == parentAssimilationPath) Seq() else pathSeq(ap.parent.get) :+ ap
      val relevantAps = pathSeq(this).iterator
      val relativeAp = relevantAps.foldLeft(AssimilationPath(None, relevantAps.next.tip))((parent, ap) => AssimilationPath(Some(parent), ap.tip))
      RelativeAssimilationPath(relativeAp.asInstanceOf[AssimilationPath[T]], parentAssimilationPath.tipDataTypeOption.isDefined)
    }
  def isChildOf(assimilationPath: AssimilationPath[_]) = {
      def recurse(currentAp: AssimilationPath[_]): Boolean = 
        if (currentAp == assimilationPath) true
        else currentAp.parent match {
          case None => false
          case Some(parentAp) => recurse(parentAp)
        }
      recurse(this)
    }
  def isReferenceToOrientatingDataType(implicit model: Model) = tipDataTypeOption match {
      case None => false
      case Some(dataType) => dataType.isEffectivelyOrientating && !assimilations.isEmpty
  }
  lazy val length = (assimilations ++ tipDataTypeOption).size
  override def toString = {
    def descriptor(dataType: DataType) = s"${dataType.name} (${dataType.filePath})"
    (assimilations.map(a => s"${descriptor(a.dataType)} -> ${a.assimilation.name.getOrElse("<ANONYMOUS>")}") ++ tipDataTypeOption.map(descriptor)).mkString(" => ")
  }
}
case class RelativeAssimilationPath[T](assimilationPath: AssimilationPath[T], startOnAssimilation: Boolean)

object AssimilationPath {
  def apply(rootDataType: DataType): AssimilationPath[DataType] = AssimilationPath[DataType](None, Left(rootDataType))
}

case class Model(definedDataTypes: Set[DataType], defaultMinimumOccurences: Int = 0, defaultMaximumOccurences: Option[Int] = Some(1)) {
  implicit val _ = this
  val dataTypes = definedDataTypes ++ Preset.DataType.All
  def dataTypeOption(filePath: FilePath.Absolute) = dataTypes.find(_.filePath == filePath)
  lazy val orientatingDataTypes = {
    val initialOrientatingDataTypes = {
      val defined = dataTypes.filter(_.isOrientating)
      val selfLooping = dataTypes.filter(dt => dt.assimilations.flatMap(_.absoluteDataTypeReferences(dt)).find(_.filePath == dt.filePath).isDefined)
      rootDataTypes ++ defined ++ selfLooping
    }
    def findLoops(alreadyCrossedDataTypes: Set[DataType], dataType: DataType): Set[Set[DataType]] = {
      if (initialOrientatingDataTypes.contains(dataType)) Set()
      else if (alreadyCrossedDataTypes.contains(dataType)) Set(alreadyCrossedDataTypes)
      else dataType.absoluteAssimilations.flatMap(_.dataTypes).foldLeft(Set.empty[Set[DataType]]) {
        (loops, childDt) => loops ++ findLoops(alreadyCrossedDataTypes + dataType, childDt)
      }
    }
    val loops = initialOrientatingDataTypes.foldLeft(Set.empty[Set[DataType]]) {
      (childLoops, iodt) => childLoops ++ findLoops(Set(), iodt)
    }
    val selectedLoopBreakingDataTypes = loops.toSeq.sortBy(_.size).foldLeft(Set.empty[DataType]) {
      (chosenOrientatingDataTypes, loop) =>
        if (loop.foldLeft(false)((dataTypeAlreadySelected, loopDt) => dataTypeAlreadySelected || chosenOrientatingDataTypes.contains(loopDt))) chosenOrientatingDataTypes
        else {
          val mostAssimilatedDataTypes = loop.groupBy(_.selfAssimilations.size).toSeq.sortBy(-_._1).map(_._2).head
          if (mostAssimilatedDataTypes.size == 1) chosenOrientatingDataTypes + mostAssimilatedDataTypes.head
          else throw new IllegalStateException(s"Cannot orientate model - please make one of the following data types orientating: ${loop.map(_.filePath).mkString(", ")}")
        }
    }
    initialOrientatingDataTypes ++ selectedLoopBreakingDataTypes
  }
  def rootDataTypes = dataTypes.filter(dt => dt.selfAssimilations.isEmpty && !dt.preset)
  def effectiveStrength(absoluteAssimilation: AbsoluteAssimilation, dataTypeReference: DataTypeReferenceLike)(implicit model: Model): AssimilationStrength = {
    val absoluteDtr = dataTypeReference.toAbsolute(absoluteAssimilation)
    val selfLooping = dataTypes.filter(dt => dt.assimilations.flatMap(_.absoluteDataTypeReferences(dt)).find(_.filePath == dt.filePath).isDefined)
    
    def assimilationStrength(dataType: DataType): AssimilationStrength = ???
      
  // Assimilations of Self-Referencing DataTypes are either weak or reference (Depending on what self-reference is and if orientating data type)
  // Unresolved assimilation loops containing no bounds should simply be registered: Any assimilation of a data type in the loop should be:
  // - Weak if not specified
  // - Never Strong
  // - Can be set to reference
  // Everything else is effectively defined
    
    
    ???
  }
  
  def hasDescendentThat(dataType: DataType, predicate: DataType => Boolean): Boolean = {
    def recurse(nextDt: DataType, visitedDtsMap: Map[DataType, Boolean]): Boolean = {
      if (visitedDtsMap.containsKey(nextDt)) visitedDtsMap(nextDt)
      else {
        val nextResult = predicate(nextDt)
        if (nextResult) true
        else {
          val nextVisitedDtsMap = visitedDtsMap + (nextDt -> nextResult)
          dataType.absoluteAssimilations.flatMap(_.dataTypes).foldLeft(false)((out, dt) => out || recurse(dt, nextVisitedDtsMap))
        }
      }
    }
    recurse(dataType, Map())
  }
}
