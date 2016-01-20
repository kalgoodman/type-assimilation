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
  def dataType(implicit model: Model): DataType = model.dataTypeOption(filePath).get
}
case class Assimilation(name: Option[String], description: Option[String], isIdentifying: Boolean, dataTypeReferences: Seq[DataTypeReference], minimumOccurences: Option[Int], maximumOccurences: Option[Int], multipleOccurenceName: Option[String]) {
  def absoluteDataTypeReferences(dataType: DataType) = dataTypeReferences.map(dtr => AbsoluteDataTypeReference(dtr.filePath.toAbsoluteUsingBase(dataType.filePath.parent.get), dtr.strength))
  def maximumMultiplicityRange(implicit model: Model) = model.maximumMultiplicityRange(this)
  override def toString = name.getOrElse("<ANONYMOUS>") + multipleOccurenceName.map(mon => s"($mon)").getOrElse("") + description.map(d => s" '$d'").getOrElse("") + s" -> [${dataTypeReferences.map(dtr => dtr.filePath).mkString(", ")}] {${minimumOccurences.getOrElse("*")},${maximumOccurences.getOrElse("*")}}"
}
case class AbsoluteAssimilation(dataType: DataType, assimilation: Assimilation) {
  def dataTypeReferences = assimilation.absoluteDataTypeReferences(dataType)
  def dataTypes(implicit model: Model) = dataTypeReferences.flatMap(dtr => model.dataTypeOption(dtr.filePath))
  override def toString = s"${dataType.name} (${dataType.filePath}) -> ${assimilation}"
}

case class RelativeJoinedAssimilationPath[J <: JoinedAssimilationPath](japOption: Option[J] = None) {
  def isEmpty = japOption.isDefined
  def joinedAssimilationPath = japOption.get
}
object RelativeJoinedAssimilationPath {
  def apply[J <: JoinedAssimilationPath](joinedAssimilationPath: J): RelativeJoinedAssimilationPath[J] = RelativeJoinedAssimilationPath(Some(joinedAssimilationPath))
}
case class RelativeAssimilationPath[A <: AssimilationPath](apOption: Option[AssimilationPath] = None) {
  def isEmpty = apOption.isDefined
  def assimilationPath = apOption.get
  def toSeq = if(isEmpty) Seq() else assimilationPath.toSeq
}
object RelativeAssimilationPath {
  def apply[A <: AssimilationPath](assimilationPath: AssimilationPath): RelativeAssimilationPath[A] = RelativeAssimilationPath(Some(assimilationPath))
}

// TODO Glean any remaining value from this code and remove
//case class AssimilationPathOld[T](parent: Option[AssimilationPath[Nothing]], tip: Either[DataType, AbsoluteAssimilation]) {
//  def ancestorThat(predicate: (Either[DataType, AbsoluteAssimilation]) => Boolean): Option[AssimilationPath[_]] = {
//    def recurse(currentAp: AssimilationPath[_]): Option[AssimilationPath[_]] = 
//       if (predicate(currentAp.tip)) Some(currentAp)
//       else currentAp.parent.flatMap(recurse(_)) 
//    recurse(this)
//  }
//  def descendentsThat(predicate: (Either[DataType, AbsoluteAssimilation]) => Boolean)(implicit model: Model): Seq[AssimilationPath[_]] =
//    model.descendentsThat(this, predicate)
//  def tipStrengthOption: Option[AssimilationStrength] = tip match {
//    case Left(dt) => assimilationParent.flatMap(_.tipAssimilation.dataTypeReferences.find(_.filePath == dt.filePath).flatMap(_.strength))
//    case Right(a) => throw new IllegalStateException(s"Only DataType AssimilationPaths can have tip defined assimilation strength.")
//  }
//  def effectiveStrength(dataTypeReference: AbsoluteDataTypeReference)(implicit model: Model): AssimilationStrength =
//    dataTypeReference.strength match {
//      case Some(strength) => strength
//      case None => tip match {
//        case Left(dataType) => throw new IllegalStateException(s"The current must be an assimilation to assess a data type reference strength.")
//        case Right(assimilation) =>
//          if (dataTypeReference.dataType.isOrientating) AssimilationStrength.Reference
//          else {
//            val forwardLoopingRaps = descendentsThat(_ match {
//              case Right(a) => a.dataTypeReferences.find(_.filePath == dataTypeReference.filePath).isDefined
//              case _ => false
//            }).map(_.relativeTo(this))
//            val loopParentOption = ancestorThat(_ match {
//              case Right(a) => a.dataTypeReferences.find(_.filePath == dataTypeReference.filePath).isDefined
//              case _ => false
//            })
//            ???
//          }
//      } 
//    }
//}


sealed trait AssimilationPath {
  import AssimilationPath._
  def tipEither: Either[DataType, AbsoluteAssimilation]
  def parent: Option[AssimilationPath]
  def children(implicit model: Model): Iterable[AssimilationPath]
  def singleChild(implicit model: Model): AssimilationPath
  def hasSingleChild: Boolean
  def tipDisplayName: String
  def tipDisplayDescription: Option[String]
  def indexOf(predicate: AssimilationPath => Boolean): Int = {
    def recurse(currentAp: AssimilationPath, index: Int): Int = 
      if (predicate(currentAp)) index
      else currentAp.parent match {
        case None => -1
        case Some(p) => recurse(p, index - 1)
      }
    recurse(this, length - 1)
  }
  def covers(that: AssimilationPath): Boolean =
    tipEither == that.tipEither &&
    length == that.length &&
    (toSeq zip that.toSeq).foldLeft(true) {
      case (finalComparison, (thisApe, thatApe)) => finalComparison && (thisApe.tipEither == that.tipEither) && ((thisApe, thatApe) match {
        case (thisAt: AssimilationTip, thatAt: AssimilationTip) => thisAt.multiplicityRange.contains(thatAt.multiplicityRange)
        case _ => true
      })
    }
  def isChildOf(ap: AssimilationPath): Boolean = indexOf(_ == ap) > -1
  def substituteParent(parent: AssimilationPath): AssimilationPath =
    indexOf(_.tipEither == parent.tipEither) match {
      case -1 => throw new IllegalArgumentException(s"No substitutable parent found.")
      case n => AssimilationPath(Some(parent), toSeq.drop(n + 1))
    }
  def withParent(parent: AssimilationPath): AssimilationPath
  def withNoParent: AssimilationPath
  def +[A <: AssimilationPath](relativeAssimilationPath: RelativeAssimilationPath[A])(implicit model: Model): A =
    relativeAssimilationPath.apOption match {
      case None => throw new IllegalArgumentException(s"Cannot add an empty relative path as there is no gurantee of type consistency.")
      case Some(ap) => AssimilationPath(Some(this), ap.toSeq).asInstanceOf[A]
    } 
  def relativeTo(parentAp: AssimilationPath): RelativeAssimilationPath[this.type] = if (isChildOf(parentAp)) {
    val relativePathIter = toSeq.drop(parentAp.toSeq.size).iterator
    if (relativePathIter.isEmpty) RelativeAssimilationPath[this.type]()
    else RelativeAssimilationPath[this.type](AssimilationPath(None, relativePathIter.toSeq))
  } else throw new IllegalArgumentException(s"$this is not a child of $parentAp.")
  lazy val toSeq = {
    def recurse(currentAp: AssimilationPath): Seq[AssimilationPath] = currentAp.parent match {
      case Some(parent) => recurse(parent) :+ currentAp
      case None => Seq(currentAp)
    }
    recurse(this)
  }
  def length = toSeq.size
  def commonTipLength(ap: AssimilationPath) = {
    def recurse(thisTip: AssimilationPath, thatTip: AssimilationPath, currentLength: Int): Int = if (thisTip.tipEither == thatTip.tipEither) {
      if (thisTip.parent.isDefined && thatTip.parent.isDefined) recurse(thisTip.parent.get, thatTip.parent.get, currentLength + 1)
      else currentLength + 1
    } else currentLength
    recurse(this, ap, 0)
  }
}
object AssimilationPath {
  case class DataTypeTip private[AssimilationPath](parent: Option[AssimilationTip], tip: DataType) extends AssimilationPath {
    def tipEither = Left(tip)
    def +<(a: Assimilation, multiplicityRange: MultiplicityRange)(implicit model: Model): AssimilationTip = AssimilationTip(Some(this), AbsoluteAssimilation(tip, a), multiplicityRange)
    def +(a: Assimilation)(implicit model: Model): AssimilationTip = +<(a, a.maximumMultiplicityRange)
    def +<(aa: AbsoluteAssimilation, multiplicityRange: MultiplicityRange)(implicit model: Model): AssimilationTip = if (aa.dataType == tip) AssimilationTip(Some(this), aa, multiplicityRange) else throw new IllegalArgumentException(s"The AbsoluteAssimilation provided '$aa' isn't for the same data type '$tip' as this AssimilationPath.")
    def +(aa: AbsoluteAssimilation)(implicit model: Model): AssimilationTip = +<(aa, aa.assimilation.maximumMultiplicityRange)
    def withParent(assimilationPath: AssimilationPath): DataTypeTip = assimilationPath match {
      case at:AssimilationTip => copy(parent = Some(at))
      case _ => throw new IllegalArgumentException(s"DataType Tip AssimilationPaths can only has Assimilation Tip parents.")
    }
    def withNoParent = copy(parent = None)
    override def substituteParent(parent: AssimilationPath): DataTypeTip = super.substituteParent(parent).asInstanceOf[DataTypeTip]
    def children(implicit model: Model): Set[AssimilationTip] = tip.absoluteAssimilations.map(this + _).toSet
    def singleChild(implicit model: Model): AssimilationTip = if (hasSingleChild) this + tip.assimilations.head else throw new IllegalStateException(s"$this has multiple child paths!")
    def hasSingleChild = tip.assimilations.size == 1
    def tipDisplayName: String = parent.flatMap(_.tip.assimilation.name).getOrElse(tip.name)
    def tipDisplayDescription: Option[String] = tip.description
    def isReferenceToOrientatingDataType(implicit model: Model) = tip.isEffectivelyOrientating && length > 1
    override def toString = parent.map(_.toString + " => ").getOrElse("") + s"${tip.name} (${tip.filePath})"
  }
  
  case class MultiplicityRange(inclusiveLowerBound: Int, inclusiveUpperBound: Option[Int]) {
    def contains(index: Int) = inclusiveUpperBound match {
      case None => index >= inclusiveLowerBound
      case Some(upper) => index >= inclusiveLowerBound && index <= upper 
    }
    def contains(range: MultiplicityRange) = inclusiveLowerBound <= range.inclusiveLowerBound && (inclusiveUpperBound match {
      case None => range.inclusiveUpperBound == None
      case Some(thisUpper) => range.inclusiveUpperBound match {
        case None => true
        case Some(thatUpper) => thatUpper <= thisUpper
      }
    })
    def bounded = inclusiveUpperBound.isDefined
    def toRange = inclusiveUpperBound match {
      case None => throw new IllegalStateException(s"Infinite range: $this")
      case Some(upper) => inclusiveLowerBound to upper
    }
    override def toString = inclusiveUpperBound match {
      case None => s"[$inclusiveLowerBound,*)"
      case Some(upper) => if (inclusiveLowerBound == upper) s"[$upper]" else s"[$inclusiveLowerBound, $upper]"
    }
  }
  object MultiplicityRange {
    def optional = MultiplicityRange(0, Some(1))
    def forOnly(index: Int) = MultiplicityRange(index, Some(index))
    def requiredUpTo(inclusiveUpperBound: Int) = MultiplicityRange(1, Some(inclusiveUpperBound))
    def optionallyUpTo(inclusiveUpperBound: Int) = MultiplicityRange(0, Some(inclusiveUpperBound))
    def from(inclusiveLowerBound: Int) = MultiplicityRange(inclusiveLowerBound, None)
    def between(inclusiveLowerBound: Int, inclusiveUpperBound: Int) = MultiplicityRange(inclusiveLowerBound, Some(inclusiveUpperBound))
    object Implicits {
      implicit def intToMultiplicityRange(index: Int) = forOnly(index)
      implicit def tupleToMultiplicityRange(tuple: (Int, Int)) = between(tuple._1, tuple._2)
    }
  }
  
  case class AssimilationTip private[AssimilationPath](parent: Option[DataTypeTip], tip: AbsoluteAssimilation, multiplicityRange: MultiplicityRange) extends AssimilationPath {
    def tipEither = Right(tip)  
    def +(dt: DataType) = DataTypeTip(Some(this), dt)
    def withParent(assimilationPath: AssimilationPath): AssimilationTip = assimilationPath match {
      case dtt: DataTypeTip => copy(parent = Some(dtt))
      case _ => throw new IllegalArgumentException(s"Assimilation Tip AssimilationPaths can only has DataType Tip parents.")
    }
    override def substituteParent(parent: AssimilationPath): AssimilationTip = super.substituteParent(parent).asInstanceOf[AssimilationTip]
    def withRange(multiplicityRange: MultiplicityRange) = copy(multiplicityRange = multiplicityRange)
    def coversRange(implicit model: Model) = multiplicityRange.contains(tip.assimilation.maximumMultiplicityRange)
    def withNoParent = copy(parent = None)
    def children(implicit model: Model): Set[DataTypeTip] = tip.dataTypes.map(this + _).toSet
    def singleChild(implicit model: Model): DataTypeTip = if (hasSingleChild) this + tip.dataTypes.head else throw new IllegalStateException(s"$this has multiple child paths!")
    def hasSingleChild = tip.dataTypeReferences.size == 1
    def tipDisplayName: String = tip.assimilation.name.getOrElse(tip.dataType.name + " Type")
    def tipDisplayDescription: Option[String] = tip.assimilation.description
    override def toString = parent.map(_.toString + " -> ").getOrElse("") + tip.assimilation.name.getOrElse("<ANONYMOUS>") + multiplicityRange.toString
  }
  def apply(dt: DataType): DataTypeTip = DataTypeTip(None, dt)
  def apply(aa: AbsoluteAssimilation)(implicit model: Model): AssimilationTip = AssimilationTip(None, aa, aa.assimilation.maximumMultiplicityRange)
  def apply(aa: AbsoluteAssimilation, multiplicityRange: MultiplicityRange)(implicit model: Model): AssimilationTip = AssimilationTip(None, aa, multiplicityRange)
  def apply(parent: Option[AssimilationPath], assimilationPathElements: Iterable[AssimilationPath]): AssimilationPath =
    if (assimilationPathElements.isEmpty) parent.getOrElse(throw new IllegalArgumentException("You cannot form an AssimilationPath with no elements and no parent"))
    else {
      val apToAddIter = assimilationPathElements.iterator
      def addDtt(parent: AssimilationTip, dtt: DataTypeTip): AssimilationPath = {
        val newDttHead = dtt.withParent(parent)
        if (apToAddIter.hasNext) addAt(newDttHead, apToAddIter.next.asInstanceOf[AssimilationTip])
        else newDttHead
      }
      def addAt(parent: DataTypeTip, at: AssimilationTip): AssimilationPath = {
        val newAtHead = at.withParent(parent)
        if (apToAddIter.hasNext) addDtt(newAtHead, apToAddIter.next.asInstanceOf[DataTypeTip])
        else newAtHead
      }
      (parent, apToAddIter.next) match {
        case (Some(parentDtt: DataTypeTip), firstAt: AssimilationTip) => addAt(parentDtt, firstAt)
        case (Some(parentAt: AssimilationTip), firstDtt: DataTypeTip) => addDtt(parentAt, firstDtt)
        case (None, first: AssimilationPath) => apply(Some(first.withNoParent), apToAddIter.toSeq)
        case _ => throw new IllegalArgumentException(s"Cannot append $assimilationPathElements to $parent.")
      }
    }
}

sealed trait JoinedAssimilationPath {
  import JoinedAssimilationPath._
  type A <: AssimilationPath
  def assimilationPaths: Set[A]
  def tipEither: Either[DataType, AbsoluteAssimilation]
  def tipDescription: Option[String] = tipEither match {
    case Left(dt) => dt.description
    case Right(aa) => aa.assimilation.description
  }
  def parents: Iterable[JoinedAssimilationPath]
  def singleChild(implicit model: Model): JoinedAssimilationPath
  def hasSingleChild: Boolean = assimilationPaths.head.hasSingleChild
  def parentToChildMap[P <: JoinedAssimilationPath](parentJap: P) = parentJap.assimilationPaths.map(pap => pap -> assimilationPaths.filter(_.isChildOf(pap)).toSet).toMap
  def childToParentMap[P <: JoinedAssimilationPath](parentJap: P) = parentToChildMap(parentJap).flatMap { case (p, cs) => cs.map(_ -> p) }.toMap
  def isChildOf(parentJap: JoinedAssimilationPath) = childToParentMap(parentJap).size == assimilationPaths.size
  def +(jap: JoinedAssimilationPath): JoinedAssimilationPath = (this, jap) match {
    case (thisJap: DataTypeTip, thatJap: DataTypeTip) => thisJap + thatJap
    case (thisJap: AssimilationTip, thatJap: AssimilationTip) => thisJap + thatJap
    case _ => throw new IllegalArgumentException("You cannot merge JoinedAssimilationPaths with different tip types.")
  }
  def +[J <: JoinedAssimilationPath](relativeJap: RelativeJoinedAssimilationPath[J])(implicit model: Model): J = relativeJap.japOption match {
    case None => throw new IllegalArgumentException("You cannot add an empty relative path to a JoinedAssimilationPath.")
    case Some(jap) if jap.assimilationPaths.size == 1 => {
      val relativeAp = RelativeAssimilationPath(jap.assimilationPaths.head)
      JoinedAssimilationPath(assimilationPaths.map(_ + relativeAp).map(_.asInstanceOf[AssimilationPath])).asInstanceOf[J]
    }
    case _ => throw new IllegalArgumentException("You can only add a relative path with exactly one AssimilationPath as it is impossible to know which parent to attach to otherwise.")
  }
  def covers(thatJap: JoinedAssimilationPath): Boolean = 
    thatJap.assimilationPaths.foldLeft(true)((overallCoverage, thatAp) => overallCoverage && 
        (assimilationPaths.foldLeft(false)((apCovered, thisAp) => apCovered || thisAp.covers(thatAp))))
  
  def substituteParent(parentJap: JoinedAssimilationPath) = JoinedAssimilationPath({
    val parentToChildSet = parentJap.assimilationPaths.map(pap => pap -> assimilationPaths.filter(_.indexOf(_.tipEither == pap.tipEither) > -1).toSet)
    parentToChildSet.flatMap { case (p, cs) => cs.map (_.substituteParent(p)) }
  })
  def commonAssimilationPath: AssimilationPath = {
    val (longest, rest) = assimilationPaths.map(_.toSeq).toSeq.sortBy(-_.size).splitAt(1)
    val commonAssimilationPathElements = longest.head.flatMap { ap =>
      import AssimilationPath._
      if (rest.foldLeft(true)((present, rap) => present && rap.find(_.tipEither == ap.tipEither).isDefined)) Some(ap)
      else None
    }
    val commonApeIter = commonAssimilationPathElements.iterator
    commonApeIter.foldLeft(commonApeIter.next.withNoParent)((parentAp, elementAp) => elementAp.withParent(parentAp))
  }
  def relativeTo[P <: JoinedAssimilationPath](parentJap: P): RelativeJoinedAssimilationPath[this.type] =
    if (!isChildOf(parentJap)) throw new IllegalArgumentException(s"$this is not a child of $parentJap!")
    else {
      val childToParent = childToParentMap(parentJap)
      val japOption = {
        val apSet = assimilationPaths.map(ap => ap.relativeTo(childToParent(ap))).flatMap(_.apOption)
        if (apSet.isEmpty) None else Some(JoinedAssimilationPath(apSet))
      }
      RelativeJoinedAssimilationPath(japOption.asInstanceOf[Option[this.type]])
    }
}

object JoinedAssimilationPath {
  case class DataTypeTip private[JoinedAssimilationPath](assimilationPaths: Set[AssimilationPath.DataTypeTip]) extends JoinedAssimilationPath {
    type A = AssimilationPath.DataTypeTip
    def tip = assimilationPaths.head.tip
    def tipEither = Left(tip)
    override lazy val commonAssimilationPath = super.commonAssimilationPath.asInstanceOf[AssimilationPath.DataTypeTip]
    def singleChild(implicit model: Model): JoinedAssimilationPath = JoinedAssimilationPath(assimilationPaths.map(_.singleChild))
    def parents: Set[AssimilationTip] = assimilationPaths.flatMap(_.parent).groupBy(_.tip).map(aps => JoinedAssimilationPath(aps._2)).toSet
    def +(aa: AbsoluteAssimilation)(implicit model: Model): AssimilationTip = JoinedAssimilationPath.AssimilationTip(assimilationPaths.map(_ + aa))
    def +(a: Assimilation)(implicit model: Model): AssimilationTip = this + AbsoluteAssimilation(tip, a)
    def +(jap: DataTypeTip) = DataTypeTip(assimilationPaths ++ jap.assimilationPaths) 
  }
  case class AssimilationTip private[JoinedAssimilationPath](assimilationPaths: Set[AssimilationPath.AssimilationTip]) extends JoinedAssimilationPath {
    type A = AssimilationPath.AssimilationTip
    def tip = assimilationPaths.head.tip
    def tipEither = Right(tip)
    override lazy val commonAssimilationPath = super.commonAssimilationPath.asInstanceOf[AssimilationPath.AssimilationTip]
    def withRange(multiplicityRange: AssimilationPath.MultiplicityRange) = copy(assimilationPaths.map(_ withRange multiplicityRange))
    def singleChild(implicit model: Model): JoinedAssimilationPath = JoinedAssimilationPath(assimilationPaths.map(_.singleChild))
    def parents: Set[DataTypeTip] = assimilationPaths.flatMap(_.parent).groupBy(_.tip).map(aps => JoinedAssimilationPath(aps._2)).toSet
    def +(dt: DataType) = JoinedAssimilationPath.DataTypeTip(assimilationPaths.map(_ + dt))
    def +(jap: AssimilationTip) = AssimilationTip(assimilationPaths ++ jap.assimilationPaths)
  }
  def apply(assimilationPaths: Set[AssimilationPath.AssimilationTip]): AssimilationTip = AssimilationTip(assimilationPaths)
  def apply(assimilationPaths: Set[AssimilationPath.DataTypeTip]): DataTypeTip = DataTypeTip(assimilationPaths)
  def apply(assimilationPaths: Set[AssimilationPath]): JoinedAssimilationPath = assimilationPaths.head match {
    case tip: AssimilationPath.DataTypeTip => apply(assimilationPaths.asInstanceOf[Set[AssimilationPath.DataTypeTip]])
    case tip: AssimilationPath.AssimilationTip => apply(assimilationPaths.asInstanceOf[Set[AssimilationPath.AssimilationTip]])
  }
  def apply(dt: DataType): DataTypeTip = apply(Set(AssimilationPath.apply(dt)))
  def apply(aa: AbsoluteAssimilation)(implicit model: Model): AssimilationTip = apply(Set(AssimilationPath.apply(aa)))
}

case class Model(definedDataTypes: Set[DataType], defaultMinimumOccurences: Int = 0, defaultMaximumOccurences: Option[Int] = Some(1)) {
  implicit val _ = this
  def maximumMultiplicityRange(assimilation: Assimilation): AssimilationPath.MultiplicityRange = AssimilationPath.MultiplicityRange(assimilation.minimumOccurences.getOrElse(defaultMinimumOccurences), assimilation.maximumOccurences.orElse(defaultMaximumOccurences))
  val dataTypes = definedDataTypes
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
  def rootDataTypes = dataTypes.filter(dt => dt.selfAssimilations.isEmpty)
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
  
// TODO Glean any remaining value from this code and remove
//  def descendentsThat(current: AssimilationPath[_], predicate: Either[DataType, AbsoluteAssimilation] => Boolean): Seq[AssimilationPath[_]] = {
//    def recurse(nextAp: AssimilationPath[_], visited: Set[Either[DataType, AbsoluteAssimilation]]): Seq[AssimilationPath[_]] = {
//      if (visited.contains(nextAp.tip)) Seq()
//      else {
//        if (predicate(nextAp.tip)) Seq(nextAp)
//        else {
//          val nextVisitedMap = visited + nextAp.tip
//          nextAp.tip match {
//            case Left(nextDt) => nextDt.absoluteAssimilations.foldLeft(Seq.empty[AssimilationPath[_]])((out, a) => out ++ recurse(nextAp + a.assimilation, nextVisitedMap))
//            case Right(nextA) => nextA.dataTypes.foldLeft(Seq.empty[AssimilationPath[_]])((out, dt) => out ++ recurse(nextAp + dt, nextVisitedMap))
//          }
//        }
//      }
//    }
//    recurse(current, Set())
//  }
}
