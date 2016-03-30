package com.typeassimilation.model

object AssimilationPathUtils {
  val DefaultAssimilationName = "Type"
  val DefaultAssimilationDescriptionPrefix = "The type of"
  def name(brokenAssimilationPath: BrokenAssimilationPath): String = {
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
      case Some(atParent: AssimilationTip) if !dtt.contiguousParent => None
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
  
  def description(brokenAssimilationPath: BrokenAssimilationPath): String = {
    import BrokenAssimilationPath._
    import WordUtils._
    def multiplicityPrefix(bap: BrokenAssimilationPath) = bap match {
      case at:AssimilationTip if !at.coversRange => Some(s"the ${rankToWord(at.multiplicityRangeLimits.inclusiveLowerBound)}" + 
      (at.multiplicityRangeLimits.inclusiveUpperBound match {
        case None => " or higher"
        case Some(upper) => if (at.multiplicityRangeLimits.inclusiveLowerBound == upper) "" else s" through ${rankToWord(upper)}"  
      }))
      case _ => None
    }
    // TODO Figure out when to use the with multiplicity
    def dtElement(dtt: DataTypeTip): Option[String] = dtt.parent match {
      case Some(atParent: AssimilationTip) if dtt.contiguousParent => if (atElement(atParent).isDefined) None else {
        if (atParent.tip.assimilation.dataTypeReferences.size > 1) {
          val subTypeDescription = prepareForAppending(dtDescription(dtt.tip))
          Some(s"(applicable if ${anOrA(subTypeDescription)} $subTypeDescription)")
        } else None
      }
      case Some(atParent: AssimilationTip) if !dtt.contiguousParent => None
      case _ => Some(s" of the ${prepareForAppending(dtDescription(dtt.tip))}")
    }
    def atElement(at: AssimilationTip): Option[String] = atDescription(at.tip).map(prepareForAppending).map {
      desc => multiplicityPrefix(at) match {
        case Some(prefix) => s"of the $prefix $desc"
        case None => s"of the $desc"
      }
    }
    def element(bap: BrokenAssimilationPath) = bap match {
      case at:AssimilationTip => atElement(at)
      case dt:DataTypeTip => dtElement(dt)
    }
    def dtDescription(dt: DataType) = dt.description.getOrElse(dt.name)
    def atDescription(aa: AbsoluteAssimilation) = aa.assimilation.description.orElse(aa.assimilation.name)
    brokenAssimilationPath match {
      case at: AssimilationTip if at.length <= 2 && atDescription(at.tip).isEmpty => s"$DefaultAssimilationDescriptionPrefix ${dtDescription(at.tip.dataType)}"
      case bap => sentenceCaseAndTerminate(bap.toSeq.reverse.flatMap(element).mkString(" ").drop("of the ".length))
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