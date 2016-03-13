package com.typeassimilation.model

import scala.xml.Elem
import com.typeassimilation.implicits.XmlImplicits._
import collection.JavaConversions._
import java.io.File
import org.apache.commons.io.FileUtils
import scala.xml.PrettyPrinter
import scala.xml.XML
import com.typeassimilation.implicits.EnhancedFile._

object DataTypePersistence {
  val FileExtension = ".type.xml"
  val UnboundedLimitName = "UNBOUNDED"
  def files(directory: File): Seq[File] = directory.listFiles.filter(f => f.isFile && f.getName.endsWith(FileExtension)) ++ directory.listFiles.filter(_.isDirectory).flatMap(files)
  def toXml(dataType: DataType) =
    <type>
      <name>{ dataType.name }</name>
      { if (dataType.description.isDefined) <description>{ dataType.description.get }</description> }
      { if (dataType.isOrientating) <orientating>true</orientating> }
      {
        if (!dataType.assimilations.isEmpty) {
          <assimilations>
            {
              dataType.assimilations.map {
                a =>
                  <assimilation>
                    { if (a.name.isDefined) <name>{ a.name.get }</name> }
                    { if (a.description.isDefined) <description>{ a.description.get }</description> }
                    { if (a.isIdentifying) <identifying>true</identifying> }
                    <types>
                      {
                        a.dataTypeReferences.map { dtr =>
                          <type>
                            <file-path>{ dtr.filePath }</file-path>
														{ if (dtr.strength.isDefined) <strength>{dtr.strength.get.representation}</strength> }
                          </type>
                        }
                      }
                    </types>
                    { if (a.minimumOccurences.isDefined && a.minimumOccurences != Some(0)) <minimum-occurrence>{ a.minimumOccurences.get }</minimum-occurrence> }
                    { if (a.maximumOccurences != Some(1)) <maximum-occurrence>{ a.maximumOccurences.getOrElse(UnboundedLimitName) }</maximum-occurrence> }
                    { if (a.multipleOccurenceName.isDefined) <multiple-occurrence-name>{ a.multipleOccurenceName.get }</multiple-occurrence-name> }
                  </assimilation>
              }
            }
          </assimilations>
        }
      }
    </type>
  def fromFile(filePath: FilePath.Absolute, rootDirectory: File) = {
    val absoluteFilePath = filePath.toFile(rootDirectory)
    val elem = try {
      XML.loadFile(absoluteFilePath)
    } catch {
      case t: Throwable => throw new IllegalStateException(s"Could not load '$absoluteFilePath'.", t)
    }
    new DataType(filePath,
      elem.childElemTextOption("name").get,
      elem.childElemTextOption("description"),
      (elem \ "assimilations" \ "assimilation").toElemSeq.map {
        e =>
          Assimilation(
            e.childElemTextOption("name"),
            e.childElemTextOption("description"),
            e.childElemTextOption("identifying").map(_.toBoolean).getOrElse(false),
            (e \ "types" \ "type").toElemSeq.map {
              te => DataTypeReference(
                    te.childElemTextOption("file-path").map(FilePath(_)).get,
                    te.childElemTextOption("strength").map(AssimilationStrength(_))
                  )
            },
            e.childElemTextOption("minimum-occurrence") match {
              case None => Some(0)
              case Some(x) => Some(x.toInt)
            },
            e.childElemTextOption("maximum-occurrence") match {
              case Some(UnboundedLimitName) => None
              case None => Some(1)
              case Some(x) => Some(x.toInt)
            },
            e.childElemTextOption("multiple-occurrence-name"))
      },
      elem.childElemTextOption("orientating").map(_.toBoolean).getOrElse(false))
  }
}

object ModelPesistence {
  private val XmlPrettyPrinter = new PrettyPrinter(1000, 2)
  def xmlPrettyPrint(elem: Elem) = XmlPrettyPrinter.format(elem)
  def files(directory: File) = DataTypePersistence.files(directory)
  def writeDirectory(model: Model, directory: File): Unit = {
    files(directory).foreach(_.delete)
    model.dataTypes.foreach(dt => FileUtils.writeStringToFile(dt.filePath.toFile(directory), xmlPrettyPrint(DataTypePersistence.toXml(dt))))
  }
  def readDirectory(directory: File): Model = new Model(
    DataTypePersistence.files(directory).map(f => DataTypePersistence.fromFile(f.relativeTo(directory), directory)).toSet)
}