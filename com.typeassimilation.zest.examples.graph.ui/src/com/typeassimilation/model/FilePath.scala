package com.typeassimilation.model

import org.apache.commons.lang3.StringUtils
import java.io.File

sealed trait FilePath {
  import FilePath._
  def stringValue: String
  override def toString = stringValue
  def asAbsolute = {
    require(isInstanceOf[Absolute], s"'$stringValue' is not an absolute file path.")
    asInstanceOf[Absolute]
  }
  def asRelative = {
    require(isInstanceOf[Relative], s"'$stringValue' is not a relative file path.")
    asInstanceOf[Relative]
  }
  def asFilePath = this
  def toRelative: FilePath.Relative
  def toAbsolute: FilePath.Absolute
  def +(relativeToAdd: Relative): FilePath
  def toAbsoluteUsingBase(basePath: FilePath.Absolute) = this match {
    case FilePath.Absolute(_) => this.asAbsolute
    case FilePath.Relative(_) => basePath + this.asRelative
  }
  def contains(relativeAtTheEnd: Relative) = stringValue.endsWith("/" + relativeAtTheEnd.stringValue)
  def parent: Option[FilePath]
  def sibling(filePath: FilePath.Relative) = this.parent match {
    case None => filePath.toAbsolute
    case Some(thisParent) => thisParent + filePath
  }
  def toFile(rootDirectory: File) = new File(rootDirectory, stringValue)
}

object FilePath {
  private val RootAbsolute = Absolute("/")
  private val EmptyRelative = Relative("")
  case class Absolute private[FilePath] (val stringValue: String) extends FilePath {
    def +(relativeToAdd: Relative): Absolute = relativeToAdd match {
      case EmptyRelative => this
      case _ => this match {
        case RootAbsolute => Absolute("/" + relativeToAdd.stringValue)
        case _ => Absolute(stringValue + "/" + relativeToAdd.stringValue)
      }
    } 
    def -(parent: Absolute): Relative = {
      require(contains(parent), s"The file path '$stringValue' does not start with path '${parent.stringValue}'.")
      FilePath(stringValue.substring(parent.stringValue.length + 1)).asRelative
    }
    def -(relativeToSubtract: Relative): Absolute = {
      require(super.contains(relativeToSubtract), s"The file path '$stringValue' is does not end in relative path '${relativeToSubtract.stringValue}'.")
      stringValue.dropRight(relativeToSubtract.stringValue.length + 1) match {
        case "" => RootAbsolute
        case s => FilePath(s).asAbsolute
      }
    }
    def contains(parentAbsolute: Absolute) = stringValue.startsWith(parentAbsolute.stringValue + "/")
    def contains(filePath: FilePath): Boolean = filePath match {
      case Absolute(_) => contains(filePath.asAbsolute)
      case Relative(_) => super.contains(filePath.asRelative)
    }
    def parent: Option[Absolute] = this match {
      case RootAbsolute => None
      case afp if afp.stringValue.indexOf("/", 1) > -1 => Some(Absolute(stringValue.substring(0, stringValue.lastIndexOf("/"))))
      case _ => Some(RootAbsolute)
    }
    def toRelative = Relative(stringValue.drop(1))
    def toAbsolute = this
    override def toString = stringValue
  }
  case class Relative private[FilePath] (val stringValue: String) extends FilePath {
    def +(relativeToAdd: Relative): Relative = relativeToAdd match {
      case EmptyRelative => this
      case _ => Relative(stringValue + "/" + relativeToAdd.stringValue)
    } 
    def parent: Option[Relative] = this match {
      case EmptyRelative => None
      case _ => Some(Relative(stringValue.substring(0, stringValue.lastIndexOf("/"))))
    }
    def toRelative = this
    def toAbsolute = Absolute("/" + stringValue)
    override def toString = stringValue
  }

  def apply(filePath: String): FilePath = {
    filePath.trim match {
      case "" => EmptyRelative
      case "/" => RootAbsolute
      case _ =>
        val cleansedFilePath = StringUtils.removeEnd(filePath.replaceAllLiterally("\\", "/").trim, "/").trim.replaceAll("""\/{2,}""", "/")
        if (cleansedFilePath.startsWith("/")) Absolute(cleansedFilePath) else Relative(cleansedFilePath)
    }
  }
  trait Implicits {
    implicit def stringToFilePath(s: String) = FilePath(s)
    implicit def seqStringToSeqFilePath(strings: Seq[String]) = strings.map(FilePath(_))
    implicit def seqFilePathToSeqString(filePaths: Seq[FilePath]) = filePaths.map(_.stringValue)
    implicit def filePathToString(fp: FilePath) = fp.stringValue
  }
  object Implicits extends Implicits
}
