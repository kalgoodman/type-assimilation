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
      elem.childElemTextOption("description").get,
      (elem \ "assimilations" \ "assimilation").toElemSeq.map {
        e => AssimilationReference(e.childElemTextOption("name"), FilePath(e.childElemTextOption("file-path").get))
      })
  }
}

object AssimilationPersistence {
  val FileExtension = ".assimilation.xml"
  def files(directory: File) = directory.listFiles.filter(_.getName.endsWith(FileExtension)).toSeq
  def toXml(assimilation: Assimilation) =
    <assimilation>
			<types>
      	{ assimilation.dataTypeReferences.map(t => <file-path>{ t.filePath }</file-path>) }
			</types>
    </assimilation>
  def fromFile(filePath: FilePath.Absolute, rootDirectory: File) = {
    val elem = XML.loadFile(filePath.toFile(rootDirectory))
    new Assimilation(filePath,
      (elem \ "types" \ "file-path").toElemSeq.map(e => new DataTypeReference(FilePath(e.text))))
  }
}

object ModelPesistence {
  private val XmlPrettyPrinter = new PrettyPrinter(1000, 2)
  def xmlPrettyPrint(elem: Elem) = XmlPrettyPrinter.format(elem)
  def files(directory: File) = DataTypePersistence.files(directory) ++ AssimilationPersistence.files(directory)
  def writeDirectory(model: Model, directory: File): Unit = {
    files(directory).foreach(_.delete)
    model.dataTypes.foreach(dt => FileUtils.writeStringToFile(dt.filePath.toFile(directory), xmlPrettyPrint(DataTypePersistence.toXml(dt))))
    model.assimilations.foreach(a => FileUtils.writeStringToFile(a.filePath.toFile(directory), xmlPrettyPrint(AssimilationPersistence.toXml(a))))
  }
  def readDirectory(directory: File): Model = new Model(
    DataTypePersistence.files(directory).map(f => DataTypePersistence.fromFile(f.relativeTo(directory), directory)),
    AssimilationPersistence.files(directory).map(f => AssimilationPersistence.fromFile(f.relativeTo(directory), directory)))
}