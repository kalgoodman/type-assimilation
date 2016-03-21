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
import scala.collection.immutable.ListSet
import scalafx.beans.Observable
import java.io.File
import FilePath.Implicits._

case class DataType(val filePath: FilePath.Absolute, name: String, description: Option[String], assimilations: Seq[Assimilation], isOrientating: Boolean, modelOption: Option[Model] = None) {
  def model = modelOption.getOrElse(throw new IllegalStateException(s"$this must be assigned a model prior to any operations (by adding it to one)."))
  def absoluteAssimilations = assimilations.map(a => AbsoluteAssimilation(this, a))
  lazy val selfAssimilations = model.dataTypes.flatMap(_.absoluteAssimilations).filter(_.dataTypeReferences.map(_.filePath).contains(filePath))
  lazy val loops = model.loops(this)
  def referenceAssimilationPath = if (isEffectivelyOrientating) AssimilationPath(this)
  else {
    val weakReferences = model.inboundTraversals(this).assimilationPaths.filter(_.effectiveAssimilationStrength == Some(AssimilationStrength.Weak))
    weakReferences.size match {
      case 0 => throw new IllegalStateException(s"Could not find any AssimilationPath for referenced DataType ($name) to link to.")
      case 1 => weakReferences.head
      case x if x > 1 => throw new IllegalStateException(s"Found multiple AssimilationPaths for referenced DataType ($name) to link to: ${weakReferences.mkString(", ")}")
    }
  }
  def identifyingAssimilations = assimilations.filter(_.isIdentifying)
  lazy val isEffectivelyOrientating = isOrientating || model.inboundTraversals(this).assimilationPaths.foldLeft(true)((allReferences, ap) => allReferences && ap.effectiveAssimilationStrength == Some(AssimilationStrength.Reference))
  lazy val isReachable = isOrientating || model.reachableDataTypes.contains(this)
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
  def model = dataType.model
  def dataTypeReferences = assimilation.absoluteDataTypeReferences(dataType)
  def dataTypes = dataTypeReferences.flatMap(dtr => model.dataTypeOption(dtr.filePath))
  def referenceOption(dataType: DataType) = dataTypeReferences.find(_.filePath == dataType.filePath) 
  override def toString = s"${dataType.name} (${dataType.filePath}) -> ${assimilation}"
}

sealed trait BrokenAssimilationPath {
  def parent: Option[BrokenAssimilationPath]
  def tipEither: Either[DataType, AbsoluteAssimilation]
  def model: Model
  def contiguousParent = parent match {
    case None => false
    case Some(p) => (tipEither, p.tipEither) match {
      case (Right(thisAa), Left(parentDt)) => thisAa.dataType == parentDt
      case (Left(thisDt), Right(parentAa)) => parentAa.dataTypeReferences.map(_.filePath).contains(thisDt.filePath)
      case _ => false
    }
  }
  def withParent(parentBap: BrokenAssimilationPath): BrokenAssimilationPath
  def withNoParent: BrokenAssimilationPath
  lazy val toSeq = {
    def recurse(currentBap: BrokenAssimilationPath): Seq[BrokenAssimilationPath] = currentBap.parent match {
      case None => Seq(currentBap)
      case Some(p) => recurse(p) :+ currentBap
    } 
    recurse(this)    
  }
  def length = toSeq.size
  def &(that: BrokenAssimilationPath): Option[BrokenAssimilationPath] = {
    val thatTipToBapMap = that.toSeq.map(bap => bap.tipEither -> bap).toMap
    toSeq.foldLeft(Option.empty[BrokenAssimilationPath]) {
      (previousParent, bap) =>
        thatTipToBapMap.get(bap.tipEither) match {
          case Some(thatBap) =>
            import BrokenAssimilationPath._
            bap match {
              case dtt: DataTypeTip => Some(DataTypeTip(previousParent, dtt.tip))
              case at: AssimilationTip => Some(AssimilationTip(previousParent, at.tip, at.multiplicityRanges ++ thatBap.asInstanceOf[AssimilationTip].multiplicityRanges))
            }
          case None => previousParent
        }
    }
  }
  override def toString = parent.map {_.toString + (if (contiguousParent) " ---> " else " -||-> ") }.getOrElse("") + tipToString
  protected def tipToString: String
  def commonTipLength(that: BrokenAssimilationPath): Int =
    if (this.tipEither == that.tipEither) (this.parent, that.parent) match {
      case (Some(thisP), Some(thatP)) => 1 + thisP.commonTipLength(thatP)
      case _ => 1
    }
    else 0
}
object BrokenAssimilationPath {
  case class DataTypeTip(parent: Option[BrokenAssimilationPath], tip: DataType) extends BrokenAssimilationPath {
    val tipEither = Left(tip)
    def model = tip.model
    def withParent(parentBap: BrokenAssimilationPath) = copy(parent = Some(parentBap))
    def withNoParent = copy(parent = None)
    def tipToString = tip.name
  }
  case class AssimilationTip(parent: Option[BrokenAssimilationPath], tip: AbsoluteAssimilation, multiplicityRanges: Set[AssimilationPath.MultiplicityRange]) extends BrokenAssimilationPath {
    val tipEither = Right(tip)
    def model = tip.model
    def withParent(parentBap: BrokenAssimilationPath) = copy(parent = Some(parentBap))
    def withNoParent = copy(parent = None)
    def multiplicityRangeLimits = AssimilationPath.MultiplicityRange(multiplicityRanges)
    def coversRange = multiplicityRangeLimits.contains(tip.assimilation.maximumMultiplicityRange(tip.model))
    def tipToString = tip.assimilation.name.getOrElse("<ANONYMOUS>") + "{" + multiplicityRanges.toSeq.sortBy(_.inclusiveLowerBound).mkString(",") + "}"
  }
  def apply(assimilationPath: AssimilationPath): BrokenAssimilationPath = { 
    val apIter = assimilationPath.toSeq.iterator
    def bap(p: Option[BrokenAssimilationPath], ap: AssimilationPath) = ap match {
      case dtt: AssimilationPath.DataTypeTip => DataTypeTip(p, dtt.tip)
      case at: AssimilationPath.AssimilationTip => AssimilationTip(p, at.tip, Set(at.multiplicityRange))
    }
    apIter.foldLeft(bap(None, apIter.next))((finalBap, ap) => bap(Some(finalBap), ap))
  }
  def apply(baps: Seq[BrokenAssimilationPath]): BrokenAssimilationPath = {
    val bapsIter = baps.iterator
    baps.foldLeft(bapsIter.next.withNoParent)((finalBap, currentBap) => currentBap.withParent(finalBap))
  }
  object Implicits {
    implicit def assimilationPathToBrokenAssimilationPath(ap: AssimilationPath) = apply(ap)
  }
}

sealed trait AssimilationPath {
  import AssimilationPath._
  def tipEither: Either[DataType, AbsoluteAssimilation]
  def head = toSeq.head
  def model: Model
  def parent: Option[AssimilationPath]
  def children: Iterable[AssimilationPath]
  def singleChild: AssimilationPath
  def isAbsolute = head.tipEither match {
    case Left(dt) => dt.isEffectivelyOrientating
    case _ => false
  }
  /**
   * All parents which are the start of this loop
   */
  def loopBackStarts: Iterable[AssimilationPath] = ListSet(ancestorsThat(ap => ap.tipEither == tipEither && ap != this):_*)
  def loopsBack = !loopBackStarts.isEmpty
  /**
   * Descendant AssimilationPaths that loop back to this AssimilationPath's tip.
   */
  def descendantLoopBacks = descendantsThat(_.loopBackStarts.contains(this))
  /**
   * Compares to another AssimilationPath and decides if they represent the same loop
   */
  def isSameLoop(that: AssimilationPath): Boolean = if (that.isChildOf(this)) containsSeq(that.toSeq(this)) else false
  def isLoop = tipEither == head.tipEither
  def loopEquals(that: AssimilationPath): Boolean = that.isLoop && that.toSeq.map(_.tipEither).toSet == toSeq.map(_.tipEither).toSet
  def contains(tipEither: Either[DataType, AbsoluteAssimilation]) = lastIndexOf(_.tipEither == tipEither) > -1
  def containsSeq(thatSeq: Seq[AssimilationPath]): Boolean =
    if (thatSeq.isEmpty) true
    else {
      val firstIndex = toSeq.indexWhere(_.tipEither == thatSeq.head.tipEither)
      if (firstIndex == -1) false
      else {
        val thisSeqReduced = toSeq.drop(firstIndex).take(thatSeq.size)
        if (thisSeqReduced.size != thatSeq.size) false 
        else (thisSeqReduced zip thatSeq).foldLeft(true) { case (result, (thisAp, thatAp)) => result && thisAp.tipEither == thatAp.tipEither }
      }
    }
  def nextFollowing(assimilationPath: AssimilationPath): AssimilationPath = assimilationPath.toSeq(this).head
  def hasSingleChild: Boolean
  def tipDisplayName: String
  def tipDisplayDescription: Option[String]
  def descendantsThat(predicate: AssimilationPath => Boolean): Seq[AssimilationPath]
  def descendants: Seq[AssimilationPath]
  def ancestorsThat(predicate: AssimilationPath => Boolean): Seq[AssimilationPath] = {
    def recurse(currentAp: AssimilationPath, found: Seq[AssimilationPath]): Seq[AssimilationPath] = {
      val nowFound = if (predicate(currentAp)) found :+ currentAp else found
      currentAp.parent match {
        case None => nowFound
        case Some(p) => recurse(p, nowFound)
      }
    }
    recurse(this, Seq())
  }
  def lastIndexOf(predicate: AssimilationPath => Boolean): Int = {
    def recurse(currentAp: AssimilationPath, index: Int): Int = 
      if (predicate(currentAp)) index
      else currentAp.parent match {
        case None => -1
        case Some(p) => recurse(p, index - 1)
      }
    recurse(this, length - 1)
  }
  def indexesOf(predicate: AssimilationPath => Boolean): Seq[Int] = toSeq.zipWithIndex.filter(ap => predicate(ap._1)).map(_._2)
  def covers(that: AssimilationPath): Boolean =
    tipEither == that.tipEither &&
    length == that.length &&
    (toSeq zip that.toSeq).foldLeft(true) {
      case (finalComparison, (thisApe, thatApe)) => finalComparison && (thisApe.tipEither == thatApe.tipEither) && ((thisApe, thatApe) match {
        case (thisAt: AssimilationTip, thatAt: AssimilationTip) => thisAt.multiplicityRange.contains(thatAt.multiplicityRange)
        case _ => true
      })
    }
  def isChildOf(ap: AssimilationPath): Boolean = lastIndexOf(_ == ap) > -1
  def substituteParent(parent: AssimilationPath) =
    (lastIndexOf(_.tipEither == parent.tipEither) match {
      case -1 => throw new IllegalArgumentException(s"No substitutable parent found.")
      case n => AssimilationPath(Some(parent), toSeq.drop(n + 1))
    }).asInstanceOf[this.type]
  def withParent(parent: AssimilationPath): AssimilationPath
  def withNoParent: AssimilationPath
  def +[A <: AssimilationPath](relativeAssimilationPath: A): A = AssimilationPath(Some(this), relativeAssimilationPath.toSeq).asInstanceOf[A]
  def relativeTo(parentAp: AssimilationPath): Option[this.type] = if (isChildOf(parentAp)) {
    val relativePathIter = toSeq.drop(parentAp.toSeq.size).iterator
    if (relativePathIter.isEmpty) None
    else Some(AssimilationPath(None, relativePathIter.toSeq).asInstanceOf[this.type])
  } else throw new IllegalArgumentException(s"$this is not a child of $parentAp.")
  lazy val relativeToLastEffectiveOrientatingDataType: this.type = (lastIndexOf {
    case dtt:DataTypeTip => dtt.tip.isEffectivelyOrientating
    case _ => false
  }) match {
    case -1 => throw new IllegalStateException(s"No effectively orientating data type within [$this].")
    case 0 => this
    case index => AssimilationPath(None, toSeq.drop(index)).asInstanceOf[this.type] 
  }
  lazy val toSeq = {
    def recurse(currentAp: AssimilationPath): Seq[AssimilationPath] = currentAp.parent match {
      case Some(parent) => recurse(parent) :+ currentAp
      case None => Seq(currentAp)
    }
    recurse(this)
  }
  def toSeq(fromParent: AssimilationPath): Seq[AssimilationPath] = if (isChildOf(fromParent)) toSeq.drop(fromParent.length) else throw new IllegalArgumentException(s"The $fromParent is not a parent of $this.")
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
  object Implicits {
    implicit class AssimilationPathSet(apSet: Set[AssimilationPath]) {
      def shortestOption = apSet.toSeq.sortBy(_.length).headOption
      def shortest = shortestOption.get
      def containsTip(tipEither: Either[DataType, AbsoluteAssimilation]) = apSet.foldLeft(false)((doesContain, ap) => doesContain || ap.contains(tipEither))
    }
  }

  case class DataTypeTip private[AssimilationPath](parent: Option[AssimilationTip], tip: DataType) extends AssimilationPath {
    def tipEither = Left(tip)
    def model = tip.model
    def +<(a: Assimilation, multiplicityRange: MultiplicityRange): AssimilationTip = AssimilationTip(Some(this), AbsoluteAssimilation(tip, a), multiplicityRange)
    def +(a: Assimilation): AssimilationTip = +<(a, a.maximumMultiplicityRange(model))
    def +<(aa: AbsoluteAssimilation, multiplicityRange: MultiplicityRange): AssimilationTip = if (aa.dataType == tip) AssimilationTip(Some(this), aa, multiplicityRange) else throw new IllegalArgumentException(s"The AbsoluteAssimilation provided '$aa' isn't for the same data type '$tip' as this AssimilationPath.")
    def +(aa: AbsoluteAssimilation): AssimilationTip = +<(aa, aa.assimilation.maximumMultiplicityRange(aa.model))
    def withParent(assimilationPath: AssimilationPath): DataTypeTip = assimilationPath match {
      case at:AssimilationTip => copy(parent = Some(at))
      case _ => throw new IllegalArgumentException(s"DataType Tip AssimilationPaths can only has Assimilation Tip parents.")
    }
    def withNoParent = copy(parent = None)
    def descendantsThat(predicate: AssimilationPath => Boolean): Seq[AssimilationPath] = {
      val currentSeq = if(predicate(this)) Seq(this) else Seq()
      if (this.loopsBack) currentSeq
      else currentSeq ++ this.children.flatMap(_.descendantsThat(predicate))
    }
    def descendants = descendantsThat(_.children.isEmpty)
    override lazy val loopBackStarts = super.loopBackStarts.asInstanceOf[ListSet[DataTypeTip]]
    override def descendantLoopBacks = super.descendantLoopBacks.asInstanceOf[Seq[DataTypeTip]]
    override def nextFollowing(assimilationPath: AssimilationPath) = super.nextFollowing(assimilationPath).asInstanceOf[AssimilationTip]
    def children: Set[AssimilationTip] = tip.absoluteAssimilations.map(this + _).toSet
    def singleChild: AssimilationTip = if (hasSingleChild) this + tip.assimilations.head else throw new IllegalStateException(s"$this has multiple child paths!")
    def parentDataTypeReference = parent.flatMap(p => p.tip.dataTypeReferences.find(_.filePath == tip.filePath))
    def effectiveAssimilationStrength = parentDataTypeReference.map(parentRef => parent.get.effectiveStrength(parentRef))
    def hasSingleChild = tip.assimilations.size == 1
    def tipDisplayName: String = parent.flatMap(_.tip.assimilation.name).getOrElse(tip.name)
    def tipDisplayDescription: Option[String] = tip.description
    def isReferenceToOrientatingDataType = tip.isEffectivelyOrientating && length > 1
    override def toString = parent.map(_.toString + (parentDataTypeReference.get.strength match {
      case None => s" =(${effectiveAssimilationStrength.get})=> "
      case Some(strength) => s" =[$strength]=> "
    })).getOrElse("") + s"${tip.name}${if (tip.isOrientating) "*" else if (tip.isEffectivelyOrientating) "*'" else ""} (${tip.filePath})"
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
    def apply(multiplicityRanges: Iterable[MultiplicityRange]): MultiplicityRange = {
      def maxLimits(mr1: MultiplicityRange, mr2: MultiplicityRange) = MultiplicityRange(Math.min(mr1.inclusiveLowerBound, mr2.inclusiveLowerBound), (mr1.inclusiveUpperBound, mr2.inclusiveUpperBound) match {
        case (bound1, bound2) if bound1.isEmpty || bound2.isEmpty => None
        case (Some(upper1), Some(upper2)) => Some(Math.max(upper1, upper2))
      })
      val rangeIter = multiplicityRanges.iterator
      rangeIter.foldLeft(rangeIter.next)((combinedRange, range) => maxLimits(combinedRange, range))
    }
    object Implicits {
      implicit def intToMultiplicityRange(index: Int) = forOnly(index)
      implicit def tupleToMultiplicityRange(tuple: (Int, Int)) = between(tuple._1, tuple._2)
    }
  }
  
  case class AssimilationTip private[AssimilationPath](parent: Option[DataTypeTip], tip: AbsoluteAssimilation, multiplicityRange: MultiplicityRange) extends AssimilationPath {
    def tipEither = Right(tip)  
    implicit def model = tip.dataType.model
    def +(dt: DataType): DataTypeTip = DataTypeTip(Some(this), dt)
    def +(dtr: AbsoluteDataTypeReference): DataTypeTip = this + dtr.dataType
    def withParent(assimilationPath: AssimilationPath): AssimilationTip = assimilationPath match {
      case dtt: DataTypeTip => copy(parent = Some(dtt))
      case _ => throw new IllegalArgumentException(s"Assimilation Tip AssimilationPaths can only has DataType Tip parents.")
    }
    def withRange(multiplicityRange: MultiplicityRange) = copy(multiplicityRange = multiplicityRange)
    def coversRange = multiplicityRange.contains(tip.assimilation.maximumMultiplicityRange)
    def withNoParent = copy(parent = None)
    def descendantsThat(predicate: AssimilationPath => Boolean): Seq[AssimilationPath] = {
      val currentSeq = if(predicate(this)) Seq(this) else Seq()
      if (this.loopsBack) currentSeq
      else currentSeq ++ this.children.flatMap(_.descendantsThat(predicate))
    }
    def descendants = descendantsThat(_.children.isEmpty)
    override lazy val loopBackStarts = super.loopBackStarts.asInstanceOf[ListSet[AssimilationTip]]
    override def descendantLoopBacks = super.descendantLoopBacks.asInstanceOf[Seq[AssimilationTip]]
    override def nextFollowing(assimilationPath: AssimilationPath) = super.nextFollowing(assimilationPath).asInstanceOf[DataTypeTip]
    def nextReferenceFollowing(assimilationPath: AssimilationPath) = tip.referenceOption(nextFollowing(assimilationPath).tip).get
    def children: Set[DataTypeTip] = tip.dataTypes.map(this + _).toSet
    def singleChild: DataTypeTip = if (hasSingleChild) this + tip.dataTypes.head else throw new IllegalStateException(s"$this has multiple child paths!")
    def hasSingleChild = tip.dataTypeReferences.size == 1
    def effectiveStrength(dataTypeReference: DataTypeReferenceLike): AssimilationStrength = {
      import AssimilationStrength._
      val absoluteDtr = dataTypeReference.toAbsolute(tip)
      absoluteDtr.strength match {
        case Some(defined) => defined
        case None =>
          if (absoluteDtr.dataType.isOrientating) Reference
          else if (absoluteDtr.dataType.loops.isEmpty) Strong
          else {
            import AssimilationPath.Implicits._
            if (absoluteDtr.dataType.loops.containsTip(Left(tip.dataType))) {
              val followedDtr = this + absoluteDtr
              if (followedDtr.loopsBack && followedDtr.loopBackStarts.last.effectiveAssimilationStrength == Weak) Reference
              else Strong
            } else Weak
          }
      }
    }
    def tipDisplayName: String = tip.assimilation.name.getOrElse(tip.dataType.name + " Type")
    def tipDisplayDescription: Option[String] = tip.assimilation.description
    override def toString = parent.map(_.toString + " -> ").getOrElse("") + tip.assimilation.name.getOrElse("<ANONYMOUS>") + multiplicityRange.toString
  }
  def apply(dt: DataType): DataTypeTip = DataTypeTip(None, dt)
  def apply(aa: AbsoluteAssimilation): AssimilationTip = AssimilationTip(None, aa, aa.assimilation.maximumMultiplicityRange(aa.model))
  def apply(aa: AbsoluteAssimilation, multiplicityRange: MultiplicityRange): AssimilationTip = AssimilationTip(None, aa, multiplicityRange)
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
  def model = assimilationPaths.head.model
  def tipEither: Either[DataType, AbsoluteAssimilation]
  def tipDescription: Option[String] = tipEither match {
    case Left(dt) => dt.description
    case Right(aa) => aa.assimilation.description
  }
  def parents: Iterable[JoinedAssimilationPath]
  def singleChild: JoinedAssimilationPath
  def hasSingleChild: Boolean = assimilationPaths.head.hasSingleChild
  def parentToChildMap[P <: JoinedAssimilationPath](parentJap: P) = parentJap.assimilationPaths.map(pap => pap -> assimilationPaths.filter(_.isChildOf(pap)).toSet).toMap
  def childToParentMap[P <: JoinedAssimilationPath](parentJap: P) = parentToChildMap(parentJap).flatMap { case (p, cs) => cs.map(_ -> p) }.toMap
  def isChildOf(parentJap: JoinedAssimilationPath) = childToParentMap(parentJap).size == assimilationPaths.size
  def |(jap: JoinedAssimilationPath): JoinedAssimilationPath = (this, jap) match {
    case (thisJap: DataTypeTip, thatJap: DataTypeTip) => thisJap | thatJap
    case (thisJap: AssimilationTip, thatJap: AssimilationTip) => thisJap | thatJap
    case _ => throw new IllegalArgumentException("You cannot merge JoinedAssimilationPaths with different tip types.")
  }
  def +[J <: JoinedAssimilationPath](relativeJap: J): J = if (relativeJap.assimilationPaths.size == 1) {
    val relativeAp = relativeJap.assimilationPaths.head
    JoinedAssimilationPath(assimilationPaths.map(_ + relativeAp).map(_.asInstanceOf[AssimilationPath])).asInstanceOf[J]
  } else {
    throw new IllegalArgumentException("You can only add a relative path with exactly one AssimilationPath as it is impossible to know which parent to attach to otherwise.")
  }
  def covers(thatJap: JoinedAssimilationPath): Boolean = 
    thatJap.assimilationPaths.foldLeft(true)((overallCoverage, thatAp) => overallCoverage && 
        (assimilationPaths.foldLeft(false)((apCovered, thisAp) => apCovered || thisAp.covers(thatAp))))
  def substituteParent(parentJap: JoinedAssimilationPath) = JoinedAssimilationPath({
    val parentToChildSet = parentJap.assimilationPaths.map(pap => pap -> assimilationPaths.filter(_.lastIndexOf(_.tipEither == pap.tipEither) > -1).toSet)
    parentToChildSet.flatMap { case (p, cs) => cs.map (_.substituteParent(p)) }
  })
  def commonAssimilationPath: BrokenAssimilationPath = {
    val apIter = assimilationPaths.map(BrokenAssimilationPath(_)).iterator
    apIter.foldLeft(Option(apIter.next))((previousBap, bap) => previousBap.flatMap(_ & bap)).getOrElse(throw new IllegalStateException("JoinedAssimilationPath without a common AssimilationPath - sohuld be impossible!"))
  }  
  
  def relativeTo(parentJap: JoinedAssimilationPath): Option[this.type] =
    if (!isChildOf(parentJap)) throw new IllegalArgumentException(s"$this is not a child of $parentJap!")
    else {
      val childToParent = childToParentMap(parentJap)
      val apSet = assimilationPaths.map(ap => ap.relativeTo(childToParent(ap))).flatten
      if (apSet.isEmpty) None else Some(JoinedAssimilationPath(apSet.asInstanceOf[Set[AssimilationPath]])).asInstanceOf[Option[this.type]]
    }
  def relativeToLastEffectiveOrientatingDataType: JoinedAssimilationPath
  def splitByParent: Iterable[JoinedAssimilationPath]
}

object JoinedAssimilationPath {
  case class DataTypeTip private[JoinedAssimilationPath](assimilationPaths: Set[AssimilationPath.DataTypeTip]) extends JoinedAssimilationPath {
    type A = AssimilationPath.DataTypeTip
    def tip = assimilationPaths.head.tip
    def tipEither = Left(tip)
    override lazy val commonAssimilationPath = super.commonAssimilationPath.asInstanceOf[BrokenAssimilationPath.DataTypeTip]
    def singleChild: JoinedAssimilationPath = JoinedAssimilationPath(assimilationPaths.map(_.singleChild))
    def parents: Set[AssimilationTip] = assimilationPaths.flatMap(_.parent).groupBy(_.tip).map(aps => JoinedAssimilationPath(aps._2)).toSet
    def +(aa: AbsoluteAssimilation): AssimilationTip = JoinedAssimilationPath.AssimilationTip(assimilationPaths.map(_ + aa))
    def +(a: Assimilation): AssimilationTip = this + AbsoluteAssimilation(tip, a)
    def |(jap: DataTypeTip) = DataTypeTip(assimilationPaths ++ jap.assimilationPaths)
    def effectiveAssimilationStrength = assimilationPaths.flatMap(_.effectiveAssimilationStrength) match {
      case x if x.size > 1 => throw new IllegalStateException(s"It shouldn't be possible to merge tips with different assimilation strengths: $this {${x.mkString(",")}}")
      case x => x.headOption
    }
    lazy val relativeToLastEffectiveOrientatingDataType = JoinedAssimilationPath(assimilationPaths.map(_.relativeToLastEffectiveOrientatingDataType))
    def splitByParent = assimilationPaths.groupBy(_.head).map(aps => JoinedAssimilationPath(aps._2)).toSet
  }
  case class AssimilationTip private[JoinedAssimilationPath](assimilationPaths: Set[AssimilationPath.AssimilationTip]) extends JoinedAssimilationPath {
    type A = AssimilationPath.AssimilationTip
    def tip = assimilationPaths.head.tip
    def tipEither = Right(tip)
    override lazy val commonAssimilationPath = super.commonAssimilationPath.asInstanceOf[BrokenAssimilationPath.AssimilationTip]
    def withRange(multiplicityRange: AssimilationPath.MultiplicityRange) = copy(assimilationPaths.map(_ withRange multiplicityRange))
    def singleChild: JoinedAssimilationPath = JoinedAssimilationPath(assimilationPaths.map(_.singleChild))
    def parents: Set[DataTypeTip] = assimilationPaths.flatMap(_.parent).groupBy(_.tip).map(aps => JoinedAssimilationPath(aps._2)).toSet
    def +(dt: DataType) = JoinedAssimilationPath.DataTypeTip(assimilationPaths.map(_ + dt))
    def |(jap: AssimilationTip) = AssimilationTip(assimilationPaths ++ jap.assimilationPaths)
    lazy val relativeToLastEffectiveOrientatingDataType = JoinedAssimilationPath(assimilationPaths.map(_.relativeToLastEffectiveOrientatingDataType))
    def splitByParent = assimilationPaths.groupBy(_.head).map(aps => JoinedAssimilationPath(aps._2)).toSet
  }
  def apply(assimilationPaths: Set[AssimilationPath.AssimilationTip]): AssimilationTip = AssimilationTip(assimilationPaths)
  def apply(assimilationPaths: Set[AssimilationPath.DataTypeTip]): DataTypeTip = DataTypeTip(assimilationPaths)
  def apply(assimilationPaths: Set[AssimilationPath]): JoinedAssimilationPath = assimilationPaths.head match {
    case tip: AssimilationPath.DataTypeTip => apply(assimilationPaths.asInstanceOf[Set[AssimilationPath.DataTypeTip]])
    case tip: AssimilationPath.AssimilationTip => apply(assimilationPaths.asInstanceOf[Set[AssimilationPath.AssimilationTip]])
  }
  def apply(dt: DataType): DataTypeTip = apply(Set(AssimilationPath.apply(dt)))
  def apply(aa: AbsoluteAssimilation): AssimilationTip = apply(Set(AssimilationPath.apply(aa)))
}

case class Model(definedDataTypes: Set[DataType], defaultMinimumOccurences: Int = 0) {
  def maximumMultiplicityRange(assimilation: Assimilation): AssimilationPath.MultiplicityRange = AssimilationPath.MultiplicityRange(assimilation.minimumOccurences.getOrElse(defaultMinimumOccurences), assimilation.maximumOccurences)
  val dataTypes = definedDataTypes.map(_.copy(modelOption = Some(this)))
  def dataTypeOption(filePath: FilePath.Absolute) = dataTypes.find(_.filePath == filePath)
  lazy val orientatingDataTypes = dataTypes.filter(_.isOrientating)
  lazy val effectivelyOrientatingDataTypes = dataTypes.filter(dt => dt.isReachable && dt.isEffectivelyOrientating)
  lazy val traversals = orientatingDataTypes.flatMap(dt => AssimilationPath(dt).descendants).map(_.asInstanceOf[AssimilationPath.DataTypeTip])
  lazy val crossingTraversals = {
    for {
      traversal <- traversals.toSeq
      element <- traversal.toSeq
    } yield element.tipEither -> traversal
  }.groupBy(_._1).mapValues(_.map(_._2).toSet)
  lazy val reachableDataTypes = dataTypes.filter(dt => crossingTraversals.contains(Left(dt)))
  lazy val loops = crossingTraversals.flatMap { case (Left(dt), traversals) => Some(dt -> traversals); case _ => None }.map {
    case (dt, traversals) => dt -> traversals.filter(_.loopsBack).map(lb => lb.indexesOf(_.tipEither == Left(dt)).toList match {
      case first :: second :: _ => AssimilationPath(None, lb.toSeq.subList(first, second + 1))
    })
  }
  def crossingTraversals(dt: DataType): Set[AssimilationPath.DataTypeTip] = if (dt.isReachable) crossingTraversals(Left(dt)) else throw new IllegalArgumentException(s"The data type is not reachable: $dt")
  def crossingTraversals(aa: AbsoluteAssimilation): Set[AssimilationPath.DataTypeTip] = crossingTraversals(Right(aa))
  lazy val inboundTraversals = crossingTraversals.map {
    case (tipEither, traversals) => tipEither -> JoinedAssimilationPath(traversals.flatMap(_.toSeq.filter(_.tipEither == tipEither)))
  }
  def inboundTraversals(dt: DataType): JoinedAssimilationPath.DataTypeTip = inboundTraversals(Left(dt)).asInstanceOf[JoinedAssimilationPath.DataTypeTip]
  def inboundTraversals(aa: AbsoluteAssimilation): JoinedAssimilationPath.AssimilationTip = inboundTraversals(Right(aa)).asInstanceOf[JoinedAssimilationPath.AssimilationTip]
  def rootDataTypes = dataTypes.filter(dt => dt.selfAssimilations.isEmpty)  
}
