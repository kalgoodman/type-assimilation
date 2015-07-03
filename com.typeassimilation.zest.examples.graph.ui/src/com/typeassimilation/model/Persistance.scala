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
  def files(directory: File) = directory.listFiles.filter(_.getName.endsWith(FileExtension)).toSeq
  def toXml(dataType: DataType) =
    <type>
      <name>{ dataType.name() }</name>
      <description>{ dataType.description() }</description>
      {
        if (!dataType.assimilationReferences.isEmpty) {
          <assimilations>
            {
              dataType.assimilationReferences.map {
                ar =>
                  <assimilation>
                    { if (ar.name.isDefined) <name>{ ar.name.get }</name> }
                    <file-path>{ ar.filePath }</file-path>
										{ if (ar.minimumOccurences.isDefined) <minimum-occurrence>{ar.minimumOccurences.get}</minimum-occurrence> }
                    { if (ar.maximumOccurences.isDefined) <maximum-occurrence>{ar.maximumOccurences.get}</maximum-occurrence> }
                    { if (ar.multipleOccurenceName.isDefined) <multiple-occurrence-name>{ar.multipleOccurenceName.get}</multiple-occurrence-name> }
                  </assimilation>
              }
            }
          </assimilations>
        }
      }
    </type>
  def fromFile(filePath: FilePath.Absolute, rootDirectory: File) = {
    val elem = XML.loadFile(filePath.toFile(rootDirectory))
    new DataType(filePath,
      elem.childElemTextOption("name").get,
      elem.childElemTextOption("description").getOrElse(""),
      (elem \ "assimilations" \ "assimilation").toElemSeq.map {
        e => AssimilationReference(
            e.childElemTextOption("name"),
            e.childElemTextOption("description"),
            FilePath(e.childElemTextOption("file-path").get),
            e.childElemTextOption("minimum-occurrence").map(_.toInt),
            e.childElemTextOption("maximum-occurrence").map(_.toInt),
            e.childElemTextOption("multiple-occurrence-name")
       )
      })
  }
}

object AssimilationPersistence {
  val FileExtension = ".assimilation.xml"
  def files(directory: File) = directory.listFiles.filter(_.getName.endsWith(FileExtension)).toSeq
  def toXml(assimilation: Assimilation) =
    <assimilation>
			{ if (assimilation.name.isDefined) <name>{assimilation.name.get}</name> }
      { if (assimilation.description.isDefined) <description>{assimilation.description.get}</description> }
      <types>
      	{ assimilation.dataTypeReferences.map(t => <file-path>{ t.filePath }</file-path>) }
			</types>
    </assimilation>
  def fromFile(filePath: FilePath.Absolute, rootDirectory: File) = {
    val elem = XML.loadFile(filePath.toFile(rootDirectory))
    new Assimilation(filePath,
      elem.childElemTextOption("name"),
      elem.childElemTextOption("description"),
      (elem \ "types" \ "file-path").toElemSeq.map(e => new DataTypeReference(FilePath(e.text))))
  }
}

object ModelPesistence {
  private val XmlPrettyPrinter = new PrettyPrinter(1000, 2)
  def xmlPrettyPrint(elem: Elem) = XmlPrettyPrinter.format(elem)
  def files(directory: File) = DataTypePersistence.files(directory) ++ AssimilationPersistence.files(directory)
  def writeDirectory(model: Model, directory: File): Unit = {
    files(directory).foreach(_.delete)
    model.dataTypes.filter(!Preset.DataType.All.contains(_)).foreach(dt => FileUtils.writeStringToFile(dt.filePath.toFile(directory), xmlPrettyPrint(DataTypePersistence.toXml(dt))))
    model.assimilations.filter(!Preset.Assimilation.All.contains(_)).foreach(a => FileUtils.writeStringToFile(a.filePath.toFile(directory), xmlPrettyPrint(AssimilationPersistence.toXml(a))))
  }
  def readDirectory(directory: File): Model = new Model(
    DataTypePersistence.files(directory).map(f => DataTypePersistence.fromFile(f.relativeTo(directory), directory)),
    AssimilationPersistence.files(directory).map(f => AssimilationPersistence.fromFile(f.relativeTo(directory), directory)))
}