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
  def files(directory: File) = directory.listFiles.filter(_.getName.endsWith(FileExtension)).toSeq
  def toXml(dataType: DataType) =
    <type>
      <name>{ dataType.name }</name>
      { if (dataType.description.isDefined) <description>{ dataType.description }</description> }
      { if (dataType.definedOrientating) <orientating>true</orientating> }
      {
        if (!dataType.assimilations.isEmpty) {
          <assimilations>
            {
              dataType.assimilations.map {
                a =>
                  <assimilation>
                    { if (a.name.isDefined) <name>{ a.name.get }</name> }
                    { if (a.description.isDefined) <description>{ a.description.get }</description> }
                    { if (a.identifying) <identifying>true</identifying> }
                    <types>
											{ a.dataTypeFilePaths.map(dtfp => <file-path>{ dtfp }</file-path>) }
										</types>
										{ if (a.minimumOccurences.isDefined && a.minimumOccurences != Some(0)) <minimum-occurrence>{a.minimumOccurences.get}</minimum-occurrence> }
                    { if (a.maximumOccurences != Some(1)) <maximum-occurrence>{a.maximumOccurences.getOrElse(UnboundedLimitName)}</maximum-occurrence> }
                    { if (a.multipleOccurenceName.isDefined) <multiple-occurrence-name>{a.multipleOccurenceName.get}</multiple-occurrence-name> }
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
        e => Assimilation(
            e.childElemTextOption("name"),
            e.childElemTextOption("description"),
            e.childElemTextOption("identifying").map(_.toBoolean).getOrElse(false),
            (e \ "types" \ "file-path").toElemSeq.map(fpe => FilePath(fpe.text)),
            e.childElemTextOption("minimum-occurrence") match {
              case None => Some(0)
              case Some(x) => Some(x.toInt)
            },
            e.childElemTextOption("maximum-occurrence") match {
              case Some(UnboundedLimitName) => None
              case None => Some(1)
              case Some(x) => Some(x.toInt)
            },
            e.childElemTextOption("multiple-occurrence-name")
       )
      },
      elem.childElemTextOption("orientating").map(_.toBoolean).getOrElse(false)
    )
  }
}

object ModelPesistence {
  private val XmlPrettyPrinter = new PrettyPrinter(1000, 2)
  def xmlPrettyPrint(elem: Elem) = XmlPrettyPrinter.format(elem)
  def files(directory: File) = DataTypePersistence.files(directory)
  def writeDirectory(model: Model, directory: File): Unit = {
    files(directory).foreach(_.delete)
    model.dataTypes.filter(!Preset.DataType.All.contains(_)).foreach(dt => FileUtils.writeStringToFile(dt.filePath.toFile(directory), xmlPrettyPrint(DataTypePersistence.toXml(dt))))
  }
  def readDirectory(directory: File): Model = new Model(
    DataTypePersistence.files(directory).map(f => DataTypePersistence.fromFile(f.relativeTo(directory), directory)).toSet
  )
}