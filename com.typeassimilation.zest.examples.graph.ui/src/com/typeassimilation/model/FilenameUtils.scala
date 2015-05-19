package com.typeassimilation.model

object FilenameUtils {

  val Separators = """[\s_\-\+\\\/]""".r 
  
  def toLowerCaseHyphenatedFileName(name : String) : String = name match {
    case x if Separators.findFirstIn(x).isDefined => name.replaceAll("[^A-Za-z0-9]", " ").replaceAll("\\s{2,}", " ").trim.replaceAll("\\s", "-").toLowerCase
    case x if x.toUpperCase == x => name.replaceAll("[^A-Za-z0-9]", " ").replaceAll("\\s{2,}", " ").trim.replaceAll("\\s", "-").toLowerCase
  	case name => deCamelCase(name).replaceAll("[^A-Za-z0-9]", " ").replaceAll("\\s{2,}", " ").trim.replaceAll("\\s", "-").toLowerCase
  }
  
  object CharacterSequence { def unapplySeq(s: String): Option[Seq[Char]] = Some(s) }
 
  def deCamelCase(camelCaseString: String) =
    String.valueOf(
      (camelCaseString + "A" * 2) sliding (3) flatMap {
        case CharacterSequence(c1, c2, c3) =>
          (c1.isUpper, c2.isUpper, c2.isWhitespace , c3.isUpper) match {
            case (true, false, false, _) => " " + c1
            case (false, true, _ , true) => c1 + " "
            case _ => String.valueOf(c1)
          }
      } toArray
    ).trim       
    
}