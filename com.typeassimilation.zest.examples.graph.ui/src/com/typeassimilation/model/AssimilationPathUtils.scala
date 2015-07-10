package com.typeassimilation.model

object AssimilationPathUtils {
  private def nameElement(a: AbsoluteAssimilation, nextDataTypeOption: Option[DataType]) = a.assimilation.name orElse {
    if (a.assimilation.dataTypeFilePaths.size > 1) nextDataTypeOption.map(_.name) orElse Some(a.dataType.name + " Type")
    else None
  }
  private def dummyAssimilationOption(tipDataTypeOption: Option[DataType]) = tipDataTypeOption.map(dt => AbsoluteAssimilation(dt, Assimilation(None, None, false, Seq(), None, None, None)))
  private def name(assimilationReferences: Seq[AbsoluteAssimilation], tipDataTypeOption: Option[DataType]) = {
    def tipAssimilationOption = tipDataTypeOption match {
      case None => Some(assimilationReferences.last)
      case Some(_) => None
    }
    if (assimilationReferences.isEmpty) tipDataTypeOption.map(_.name).get
    else if (assimilationReferences.size == 1) nameElement(assimilationReferences.head, tipDataTypeOption).getOrElse(throw new IllegalStateException(s"Couldn't generate name for ${assimilationReferences.head}."))
    else ({
      for {
        former :: latter :: Nil <- (assimilationReferences ++ dummyAssimilationOption(tipDataTypeOption)).sliding(2)
      } yield nameElement(former, Some(latter.dataType))
    }.toSeq.flatten ++ tipAssimilationOption.flatMap(_.assimilation.name)).mkString(" ")
  }

  def absoluteName(assimilationPath: JoinedAssimilationPath[_])(implicit model: Model): String =
    if (assimilationPath.commonLength < 2) name(assimilationPath.commonAssimilations, assimilationPath.tipDataTypeOption)
    else {
      ((assimilationPath.commonAssimilations ++ dummyAssimilationOption(assimilationPath.tipDataTypeOption)).sliding(2).foldLeft((false, Seq(""))) {
        case ((found, names), former :: latter :: Nil) =>
          val nextNameElement = nameElement(former, Some(latter.dataType))
          if (found || latter.dataType.selfAssimilations.size > 1) (true, names ++ nextNameElement)
          else (false, nextNameElement.toSeq)
      }._2 ++ assimilationPath.tipAssimilationOption.flatMap(_.assimilation.name)).mkString(" ")
    }
  def mostJoinable[T](assimilationPath: JoinedAssimilationPath[_], choices: Iterable[JoinedAssimilationPath[T]]): Set[JoinedAssimilationPath[T]] =
    choices.map {
      choice =>
        if (choice.tip != assimilationPath.tip) (0, choice)
        else {
          val (_, degree) = (assimilationPath.commonAssimilations.reverse zip choice.commonAssimilations.reverse).foldLeft((true, 0)) {
            case ((matching, degree), (assimilation, choiceAssimilation)) =>
              if (matching && assimilation == choiceAssimilation) (true, degree + 1)
              else (false, degree)
          }
          (degree, choice)
        }
    }.filter(_._1 > 0).groupBy(_._1).toSeq.sortBy(-_._1).map(_._2).headOption.toSet.flatten.map(_._2)

  def relativeName(parentAssimilationPath: JoinedAssimilationPath[_], assimilationPath: JoinedAssimilationPath[_]): String =
    name(assimilationPath.relativeTo(parentAssimilationPath).assimilationPath.commonAssimilations, assimilationPath.tipDataTypeOption)
}