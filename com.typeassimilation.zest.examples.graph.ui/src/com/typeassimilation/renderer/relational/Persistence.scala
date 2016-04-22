package com.typeassimilation.renderer.relational

import scala.xml.Elem
import com.typeassimilation.implicits.XmlImplicits._
import collection.JavaConversions._
import java.io.File
import org.apache.commons.io.FileUtils
import scala.xml.PrettyPrinter
import scala.xml.XML
import com.typeassimilation.implicits.EnhancedFile._
import com.typeassimilation.model.FilePath
import com.typeassimilation.renderer.relational.RelationalRenderer.PrimaryKeyPolicy
import com.typeassimilation.renderer.relational.RelationalRenderer.VersioningPolicy

object RelationalRendererPersistence {
  def fromFile(filePath: FilePath.Absolute, rootDirectory: File) = {
    val absoluteFilePath = filePath.toFile(rootDirectory)
    val elem = try {
      XML.loadFile(absoluteFilePath)
    } catch {
      case t: Throwable => throw new IllegalStateException(s"Could not load '$absoluteFilePath'.", t)
    }
    val defaultConfig = RelationalRenderer.Config(columnTypeMap = Map())
    RelationalRenderer.Config(
      maximumOccurencesTableLimit = elem.childElemTextOption("maximum-occurences-table-limit").map(_.toInt).getOrElse(defaultConfig.maximumOccurencesTableLimit),
      maximumSubTypeColumnsBeforeSplittingOut = elem.childElemTextOption("maximum-sub-type-columns-before-splitting-out").map(_.toInt).orElse(defaultConfig.maximumSubTypeColumnsBeforeSplittingOut),
      primaryKeyPolicy = (elem \ "primary-key-policy").childElemSeq.headOption match {
        case None => defaultConfig.primaryKeyPolicy
        case Some(skp) if skp.label == "surrogate-key-generation" => PrimaryKeyPolicy.SurrogateKeyGeneration
        case Some(nkp) if nkp.label == "natural-key" =>
          val defaultNaturalKey = PrimaryKeyPolicy.NaturalKey()
          PrimaryKeyPolicy.NaturalKey(nkp.childElemTextOption("no-key-suffix").getOrElse(defaultNaturalKey.suffix))
      },
      versioningPolicy = (elem \ "versioning-policy").childElemSeq.headOption match {
        case None => defaultConfig.versioningPolicy
        case Some(type2) if type2.label == "type-2" =>
          val defaultType2 = VersioningPolicy.Type2()
          VersioningPolicy.Type2(
            validFromColumnName = type2.childElemTextOption("valid-from-column-name").getOrElse(defaultType2.validFromColumnName),
            validToColumnName = type2.childElemTextOption("valid-to-column-name").getOrElse(defaultType2.validToColumnName),
            dateColumnType = type2.childElemTextOption("date-column-type").map(ColumnType(_)).getOrElse(defaultType2.dateColumnType))
        case Some(nearestOrientating) if nearestOrientating.label == "nearest-orientating" =>
          val defaultNearestOrientating = VersioningPolicy.NearestOrientating()
          VersioningPolicy.NearestOrientating(
            revisionDttmSuffix = nearestOrientating.childElemTextOption("revision-datetime-suffix").getOrElse(defaultNearestOrientating.revisionDttmSuffix),
            dateColumnType = nearestOrientating.childElemTextOption("date-column-type").map(ColumnType(_)).getOrElse(defaultNearestOrientating.dateColumnType),
            applyToPk = nearestOrientating.childElemTextOption("apply-to-pk").map(_.toBoolean).getOrElse(defaultNearestOrientating.applyToPk),
            tieVersions = nearestOrientating.childElemTextOption("tie-versions").map(_.toBoolean).getOrElse(defaultNearestOrientating.tieVersions))
      },
      surrogateKeyColumnType = elem.childElemTextOption("surrogate-key-column-type").map(ColumnType(_)).getOrElse(defaultConfig.surrogateKeyColumnType),
      enumerationColumnType = elem.childElemTextOption("enumeration-column-type").getOrElse(defaultConfig.enumerationColumnType),
      columnTypeMap = (elem \ "column-type-mappings" \ "column-type-mapping").toElemSeq.map(me => me.childElemTextOption("type-file-path").map(FilePath(_).toAbsoluteUsingBase(filePath.parent.get)).get -> me.childElemTextOption("column-type").map(ColumnType(_)).get).toMap)
  }

}