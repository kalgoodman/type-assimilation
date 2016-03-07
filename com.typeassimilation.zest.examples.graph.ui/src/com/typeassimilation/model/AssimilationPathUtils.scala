package com.typeassimilation.model

object AssimilationPathUtils {
  val DefaultAssimilationName = "Type"
  def name(brokenAssimilationPath: BrokenAssimilationPath)(implicit model: Model): String = {
    import BrokenAssimilationPath._
    def multiplicitySuffix(bap: BrokenAssimilationPath) = bap match {
      case at:AssimilationTip if !at.coversRange => " " + at.multiplicityRangeLimits.inclusiveLowerBound.toString + 
      (at.multiplicityRangeLimits.inclusiveUpperBound match {
        case None => " or more"
        case Some(upper) => if (at.multiplicityRangeLimits.inclusiveLowerBound == upper) "" else s" to $upper"  
      })
      case _ => ""
    }
    def dtNameElement(dtt: DataTypeTip): Option[String] = dtt.parent match {
      case Some(atParent: AssimilationTip) if dtt.contiguousParent => if (atNameElement(atParent).isDefined) None else {
        if (atParent.tip.assimilation.dataTypeReferences.size > 1) Some(dtt.tip.name + multiplicitySuffix(atParent))
        else None
      }
      case Some(_) if !dtt.contiguousParent => None
      case _ => Some(dtt.tip.name)
    }
    def atNameElement(at: AssimilationTip): Option[String] = at.tip.assimilation.name.map(_ + multiplicitySuffix(at))
    def nameElement(bap: BrokenAssimilationPath) = bap match {
      case at:AssimilationTip => atNameElement(at)
      case dt:DataTypeTip => dtNameElement(dt)
    }
    brokenAssimilationPath match {
      case at: AssimilationTip if at.length <= 2 && at.tip.assimilation.name.isEmpty => s"${at.tip.dataType.name} $DefaultAssimilationName"
      case bap => bap.toSeq.flatMap(nameElement).mkString(" ")
    }
  }
  def name(tipEither: Either[DataType, AbsoluteAssimilation]): String = tipEither match {
    case Left(dt) => dt.name
    case Right(aa) => aa.assimilation.name.getOrElse(s"${name(Left(aa.dataType))} $DefaultAssimilationName")
  }
  def merge(assimilationPaths: Set[JoinedAssimilationPath]): Set[JoinedAssimilationPath] = {
    def recurse(mergedJaps: Set[JoinedAssimilationPath], remainingJaps: Set[JoinedAssimilationPath]): Set[JoinedAssimilationPath] = {
      if (remainingJaps.isEmpty) mergedJaps
      else {
        val (headSet, tailSet) = remainingJaps.splitAt(1)
        val (mergeable, notMergeable) = tailSet.partition(_.tipEither == headSet.head.tipEither)
        val newMergedJaps = mergedJaps + mergeable.foldLeft(headSet.head)((merged, next) => merged | next)
        recurse(newMergedJaps, notMergeable)
      }
    }
    recurse(Set(), assimilationPaths)
  }
  def mostJoinable[J <: JoinedAssimilationPath](assimilationPath: J, choices: Iterable[J]): Set[J] = 
    choices.map(c => c -> assimilationPath.commonAssimilationPath.commonTipLength(c.commonAssimilationPath)).groupBy(_._2).toSeq.sortBy(-_._1).headOption.map(_._2).map(_.map(_._1).toSet) match {
      case None => Set()
      case Some(result) => result
    }
}