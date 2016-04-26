package com.typeassimilation.renderer.relational

import scala.collection.mutable
import com.typeassimilation.model._

object RelationalRenderer {
  sealed trait ColumnGroup {
    type T <: ColumnGroup
    def assimilationPath: JoinedAssimilationPath
    def model = assimilationPath.model
    def withAssimilationPath(jap: JoinedAssimilationPath): T
    def technicalDescription(logicalTable: LogicalTable, prefix: String = ""): String
    def size: Int
    def canMergeWith(that: ColumnGroup): Boolean = this.getClass == that.getClass && assimilationPath.tipEither == that.assimilationPath.tipEither
    def mergeWith(that: ColumnGroup): ColumnGroup = if (canMergeWith(that)) doMerge(this.asInstanceOf[T], that.asInstanceOf[T]) else throw new IllegalStateException(s"Cannot merge ColumnGroup '$this' with '$that'.")
    protected def doMerge(thisColumnGroup: T, thatColumnGroup: T): T
  }
  object ColumnGroup {
    case class Repeated(columnGroup: ColumnGroup, repeats: Int) extends ColumnGroup {
      type T = Repeated
      def assimilationPath = columnGroup.assimilationPath
      def withAssimilationPath(jap: JoinedAssimilationPath) = copy(columnGroup = columnGroup.withAssimilationPath(jap))
      def technicalDescription(logicalTable: LogicalTable, prefix: String): String = s"$prefix${repeats} x { ${columnGroup.technicalDescription(logicalTable)} }"
      def doMerge(thisColumnGroup: Repeated, thatColumnGroup: Repeated): Repeated = thisColumnGroup.copy(columnGroup = columnGroup.mergeWith(thatColumnGroup.columnGroup))
      def size = columnGroup.size * repeats
    }
    case class NestedTable(assimilationPath: JoinedAssimilationPath, columnGroups: Seq[ColumnGroup]) extends ColumnGroup {
      type T = NestedTable
      def withAssimilationPath(jap: JoinedAssimilationPath) = copy(assimilationPath = jap, columnGroups = columnGroups.map(cg => cg.withAssimilationPath(cg.assimilationPath.substituteParent(jap))))
      def technicalDescription(logicalTable: LogicalTable, prefix: String): String = s"${prefix}NESTED: ${relativeName(logicalTable.assimilationPath, assimilationPath)} [$assimilationPath]" + (if (columnGroups.isEmpty) "" else s" {\n${columnGroups.map(_.technicalDescription(logicalTable, prefix + "  ")).mkString(s"\n")}\n$prefix}")
      def doMerge(thisColumnGroup: NestedTable, thatColumnGroup: NestedTable): NestedTable = thisColumnGroup.copy(assimilationPath = thisColumnGroup.assimilationPath | thatColumnGroup.assimilationPath, columnGroups = (thisColumnGroup.columnGroups zip thatColumnGroup.columnGroups).map {
        case (thisCg, thatCg) => thisCg.mergeWith(thatCg)
      })
      def size = columnGroups.foldLeft(0)((sum, cg) => sum + cg.size)
    }
    case class SimpleColumn(assimilationPath: JoinedAssimilationPath, columnType: ColumnType) extends ColumnGroup {
      type T = SimpleColumn
      def withAssimilationPath(jap: JoinedAssimilationPath) = copy(assimilationPath = jap)
      def technicalDescription(logicalTable: LogicalTable, prefix: String): String = s"$prefix${relativeName(logicalTable.assimilationPath, assimilationPath)} $columnType [$assimilationPath]"
      def doMerge(thisColumnGroup: SimpleColumn, thatColumnGroup: SimpleColumn): SimpleColumn = thisColumnGroup.copy(assimilationPath = thisColumnGroup.assimilationPath | thatColumnGroup.assimilationPath)
      def size = 1
    }
    case class EnumerationColumn(assimilationPath: JoinedAssimilationPath.AssimilationTip) extends ColumnGroup {
      type T = EnumerationColumn
      def withAssimilationPath(jap: JoinedAssimilationPath) = jap match { case at: JoinedAssimilationPath.AssimilationTip => copy(assimilationPath = at); case _ => throw new IllegalArgumentException(s"Must be a AssimilationTip for ColumnGroup: $this") }
      def technicalDescription(logicalTable: LogicalTable, prefix: String): String = s"${prefix}ENUMERATION: ${relativeName(logicalTable.assimilationPath, assimilationPath)} [$assimilationPath]"
      def doMerge(thisColumnGroup: EnumerationColumn, thatColumnGroup: EnumerationColumn): EnumerationColumn = thisColumnGroup.copy(assimilationPath = thisColumnGroup.assimilationPath | thatColumnGroup.assimilationPath)
      def size = 1
    }
    case class ParentReference(assimilationPath: JoinedAssimilationPath) extends ColumnGroup {
      type T = ParentReference
      def withAssimilationPath(jap: JoinedAssimilationPath) = copy(assimilationPath = jap)
      def technicalDescription(logicalTable: LogicalTable, prefix: String): String = s"${prefix}Parent reference to ${AssimilationPathUtils.name(assimilationPath.commonAssimilationPath)} [$assimilationPath]"
      def doMerge(thisColumnGroup: ParentReference, thatColumnGroup: ParentReference): ParentReference = thisColumnGroup.copy(assimilationPath = thisColumnGroup.assimilationPath | thatColumnGroup.assimilationPath)
      def size = 1
      def parentAssimilationPath = {
        val directParent = assimilationPath.parents.head
        directParent.tipEither match {
          case Left(dataType) => directParent
          case Right(assimilation) => directParent.parents.head
        }
      }
    }
    case class ChildReference(assimilationPath: JoinedAssimilationPath) extends ColumnGroup {
      type T = ChildReference
      def withAssimilationPath(jap: JoinedAssimilationPath) = copy(assimilationPath = jap)
      def technicalDescription(logicalTable: LogicalTable, prefix: String): String = s"${prefix}Child reference to ${AssimilationPathUtils.name(assimilationPath.commonAssimilationPath)} [$assimilationPath]"
      def doMerge(thisColumnGroup: ChildReference, thatColumnGroup: ChildReference): ChildReference = thisColumnGroup.copy(assimilationPath = thisColumnGroup.assimilationPath | thatColumnGroup.assimilationPath)
      def size = 1
      lazy val childAssimilationPath = assimilationPath match {
        case dtt: JoinedAssimilationPath.DataTypeTip => JoinedAssimilationPath(Set(dtt.tip.referenceAssimilationPath))
        case _ => throw new IllegalStateException(s"Currently can only deal with child references to data types - not [$assimilationPath]")
      }
      lazy val isReferenceToWeakAssimilationWithCommonParent =
        childAssimilationPath.effectiveAssimilationStrength == Some(AssimilationStrength.Weak) &&
          (assimilationPath.relativeToLastEffectiveOrientatingDataType.heads subsetOf childAssimilationPath.relativeToLastEffectiveOrientatingDataType.heads)
    }
    def apply(logicalTable: LogicalTable): ColumnGroup = {
      val columnGroups = logicalTable.columnGroups.filterNot(_.isInstanceOf[ParentReference])
      if (columnGroups.size == 1 && columnGroups.head.assimilationPath == logicalTable.assimilationPath) columnGroups.head
      else NestedTable(logicalTable.assimilationPath, columnGroups)
    }
  }
  case class LogicalTable(assimilationPath: JoinedAssimilationPath, columnGroups: Seq[ColumnGroup], modelOption: Option[LogicalRelationalModel] = None) {
    import ColumnGroup._
    def model = modelOption.getOrElse(throw new IllegalStateException(s"$this has not been added to a model yet."))
    def parentReferenceOption = columnGroups.flatMap { case par: ColumnGroup.ParentReference => Some(par); case _ => None }.headOption
    def technicalDescription = s"TABLE: ${AssimilationPathUtils.name(assimilationPath.commonAssimilationPath)} [$assimilationPath] {\n${columnGroups.map(_.technicalDescription(this, "  ")).mkString("\n")}\n}\n"
    def allColumnGroups: Seq[ColumnGroup] = {
      def recurseColumnGroups(cg: ColumnGroup): Seq[ColumnGroup] = cg match {
        case r: Repeated => Seq(r) ++ recurseColumnGroups(r.columnGroup)
        case nt: NestedTable => Seq(nt) ++ nt.columnGroups.flatMap(recurseColumnGroups(_))
        case cg => Seq(cg)
      }
      columnGroups.flatMap(recurseColumnGroups(_))
    }
    def nestedAssimilationPaths = allColumnGroups.flatMap { case nt: NestedTable => Some(nt); case _ => None }.map(_.assimilationPath).toSet
    def assimilationPaths = nestedAssimilationPaths ++ AssimilationPathUtils.merge(nestedAssimilationPaths) + assimilationPath
    def canMergeWith(parent: LogicalTable, that: LogicalTable): Boolean =
      this.assimilationPath.tipEither == that.assimilationPath.tipEither &&
        parent.assimilationPaths.contains(this.parentReferenceOption.get.parentAssimilationPath) &&
        parent.assimilationPaths.contains(that.parentReferenceOption.get.parentAssimilationPath)
    def mergeWith(parent: LogicalTable, that: LogicalTable): LogicalTable = if (canMergeWith(parent, that)) copy(assimilationPath = this.assimilationPath | that.assimilationPath, columnGroups = (this.columnGroups zip that.columnGroups).map {
      case (thisCg, thatCg) => thisCg.mergeWith(thatCg)
    })
    else throw new IllegalStateException(s"Cannot merge $this with $that.")
  }
  class LogicalRelationalModel(definedTables: Set[LogicalTable], val config: Config) {
    private val assimilationPathToTableMap: mutable.Map[JoinedAssimilationPath, LogicalTable] = mutable.Map(tables.flatMap(lt => lt.assimilationPaths.map(ap => ap -> lt)).toArray: _*)
    lazy val tables = definedTables.map(_.copy(modelOption = Some(this)))
    def logicalTable(assimilationPath: JoinedAssimilationPath) = assimilationPathToTableMap.getOrElseUpdate(assimilationPath, {
      val apToSeek = assimilationPath.relativeToLastEffectiveOrientatingDataType
      assimilationPathToTableMap.keysIterator.find(_.covers(apToSeek)) match {
        case Some(ap) => assimilationPathToTableMap(ap)
        case None => throw new IllegalArgumentException(s"There are no LogicalTables that account for AssimilationPath $assimilationPath")
      }
    })
    def follow(parentReference: ColumnGroup.ParentReference) = logicalTable(parentReference.parentAssimilationPath)
    def follow(childReference: ColumnGroup.ChildReference) = logicalTable(childReference.childAssimilationPath)
    override def toString = tables.toSeq.sortBy(t => AssimilationPathUtils.name(t.assimilationPath.commonAssimilationPath)).map(_.technicalDescription).mkString("\n")
  }
  object LogicalRelationalModel {
    def apply(tables: Set[LogicalTable], config: Config) = new LogicalRelationalModel(tables, config)
  }

  case class LogicalTableSet(table: LogicalTable, childLogicalTableSets: Set[LogicalTableSet]) {
    def toSet: Set[LogicalTable] = childLogicalTableSets.flatMap(_.toSet) + table
    def canMergeWith(parent: LogicalTable, that: LogicalTableSet) = table.canMergeWith(parent, that.table)
    def mergeWith(parent: LogicalTable, that: LogicalTableSet)(implicit config: Config): LogicalTableSet = {
      def recurse(p: LogicalTable, ts1: LogicalTableSet, ts2: LogicalTableSet): LogicalTableSet = {
        LogicalTableSet(
          table = ts1.table.mergeWith(p, ts2.table),
          childLogicalTableSets = {
            val cts2Map = ts2.childLogicalTableSets.map(cts2 => cts2.table.assimilationPath -> cts2).toMap
            def failSelectingEquivalentChild(cts1: LogicalTableSet) = throw new IllegalStateException(s"Could not find equivalent child to ${cts1.table.technicalDescription} in ${ts2.childLogicalTableSets.map(_.table.technicalDescription).mkString(",")}.")
            ts1.childLogicalTableSets.map { cts1 =>
              val ts2EquivalentAp = AssimilationPathUtils.mostJoinable(cts1.table.assimilationPath, cts2Map.keys).headOption.getOrElse(failSelectingEquivalentChild(cts1))
              recurse(table, cts1, cts2Map.getOrElse(ts2EquivalentAp, failSelectingEquivalentChild(cts1)))
            }
          })
      }
      recurse(parent, this, that)
    }
  }

  object Implicits {
    case class EnhancedColumnReference(columnReference: ColumnReference) {
      def table(implicit model: RelationalModel) = model.table(columnReference.tableName)
      def column(implicit model: RelationalModel) = table.flatMap(_.column(columnReference.columnName))
    }
    implicit def columnReferenceToEnhancedColumnReference(columnReference: ColumnReference) = EnhancedColumnReference(columnReference)
  }

  case class Config(
    maximumOccurencesTableLimit: Int = 4,
    maximumSubTypeColumnsBeforeSplittingOut: Option[Int] = Some(10),
    namingPolicy: NamingPolicy = NamingPolicy.Default,
    primaryKeyPolicy: PrimaryKeyPolicy = PrimaryKeyPolicy.SurrogateKeyGeneration,
    versioningPolicy: VersioningPolicy = VersioningPolicy.None,
    surrogateKeyColumnType: ColumnType = ColumnType("NUMBER(15)"),
    enumerationColumnType: String = "VARCHAR([SIZE])",
    columnTypeMap: Map[FilePath.Absolute, ColumnType]) {
    def mappedColumnType(dt: DataType) = columnTypeMap.get(dt.filePath)
  }

  trait NamingPolicy {
    def tableName(logicalTable: LogicalTable)(implicit config: Config): String
    def tableDescription(logicalTable: LogicalTable)(implicit config: Config): Option[String]
    def columnName(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath)(implicit config: Config): String
    def columnDescription(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath)(implicit config: Config): Option[String]
    def foreignKeyColumnName(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath, referencePrimaryKeyColumn: Column)(implicit config: Config): String
    def foreignKeyColumnDescription(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath, referencePrimaryKeyColumn: Column)(implicit config: Config): Option[String]
    def parentForeignKeyColumnName(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath, referencePrimaryKeyColumn: Column)(implicit config: Config): String
    def parentForeignKeyColumnDescription(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath, referencePrimaryKeyColumn: Column)(implicit config: Config): Option[String]
    def enumerationName(assimilationPath: JoinedAssimilationPath)(implicit config: Config): String
    def enumerationValueName(assimilationPath: JoinedAssimilationPath)(implicit config: Config): String
    def enumerationValueDescription(assimilationPath: JoinedAssimilationPath)(implicit config: Config): Option[String]
  }

  object NamingPolicy {
    object Default extends NamingPolicy {
      import WordUtils._
      val shorten = Map(
        "IDENTIFIER" -> "ID",
        "CODE" -> "CD",
        "NAME" -> "NM",
        "NAMES" -> "NM",
        "DATE" -> "DT",
        "DATETIME" -> "DTTM",
        "TIME" -> "TM",
        "NUMBER" -> "NO",
        "COUNT" -> "CNT").withDefault(s => s)
      private def underscoreDelimitedName(s: String) = s.trim.replaceAll("\\s+", "_").toUpperCase.split('_').map(shorten).mkString("_")
      def tableName(logicalTable: LogicalTable)(implicit config: Config): String = underscoreDelimitedName(AssimilationPathUtils.name(logicalTable.assimilationPath.commonAssimilationPath))
      def tableDescription(logicalTable: LogicalTable)(implicit config: Config): Option[String] = logicalTable.assimilationPath.tipDescription
      def columnName(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath)(implicit config: Config): String = underscoreDelimitedName(relativeName(parentLogicalTable.assimilationPath, assimilationPath))
      def columnDescription(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath)(implicit config: Config): Option[String] = Some(relativeDescription(parentLogicalTable.assimilationPath, assimilationPath))
      def foreignKeyColumnName(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath, referencePrimaryKeyColumn: Column)(implicit config: Config): String = columnName(parentLogicalTable, assimilationPath) + "_" + referencePrimaryKeyColumn.name
      def foreignKeyColumnDescription(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath, referencePrimaryKeyColumn: Column)(implicit config: Config): Option[String] = referencePrimaryKeyColumn.description.map(prepareForAppending).map("Reference to " + _).map(sentenceCaseAndTerminate)
      def parentForeignKeyColumnName(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath, referencePrimaryKeyColumn: Column)(implicit config: Config): String = referencePrimaryKeyColumn.name
      def parentForeignKeyColumnDescription(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath, referencePrimaryKeyColumn: Column)(implicit config: Config): Option[String] = referencePrimaryKeyColumn.description.map(prepareForAppending).map("Reference to " + _).map(sentenceCaseAndTerminate)
      def enumerationName(assimilationPath: JoinedAssimilationPath)(implicit config: Config): String = AssimilationPathUtils.name(assimilationPath.tipEither).replaceAll("\\s+", "")
      def enumerationValueName(assimilationPath: JoinedAssimilationPath)(implicit config: Config): String = underscoreDelimitedName(AssimilationPathUtils.name(assimilationPath.tipEither))
      def enumerationValueDescription(assimilationPath: JoinedAssimilationPath)(implicit config: Config): Option[String] = assimilationPath.tipDescription
    }
  }

  trait PrimaryKeyPolicy {
    def isPrimaryKeyColumnGroup(columnGroup: ColumnGroup, logicalTable: LogicalTable): Boolean
    def modifyPrimaryKeys(currentPrimaryKeys: Seq[Column], tableName: String, logicalTable: LogicalTable): Seq[Column]
  }

  object PrimaryKeyPolicy {
    object SurrogateKeyGeneration extends PrimaryKeyPolicy {
      import ColumnGroup._
      private def isManyToMany(logicalTable: LogicalTable) = logicalTable.parentReferenceOption.flatMap(pr => logicalTable.columnGroups.find {
        case cr: ChildReference => cr.assimilationPath == pr.assimilationPath
        case _ => false
      }).isDefined
      def isPrimaryKeyColumnGroup(columnGroup: ColumnGroup, logicalTable: LogicalTable): Boolean = isManyToMany(logicalTable) && (columnGroup match {
        case pr: ParentReference => true
        case cr: ChildReference => cr.assimilationPath == logicalTable.parentReferenceOption.get.assimilationPath
        case _ => false
      })
      def modifyPrimaryKeys(currentPrimaryKeys: Seq[Column], tableName: String, logicalTable: LogicalTable): Seq[Column] = if (currentPrimaryKeys.isEmpty) Seq(Column(tableName + "_SK", Some(s"Surrogate Key (system generated) for the ${tableName} table."), logicalTable.model.config.surrogateKeyColumnType, true, false, logicalTable.assimilationPath, true)) else currentPrimaryKeys
    }
    case class NaturalKey(suffix: String = "_ID") extends PrimaryKeyPolicy {
      private def hasIdentifyingColumnGroup(logicalTable: LogicalTable) = logicalTable.columnGroups.exists(cg => isIdentifyingColumnGroup(cg, logicalTable))
      private def isIdentifyingColumnGroup(columnGroup: ColumnGroup, logicalTable: LogicalTable) = columnGroup.assimilationPath.relativeTo(logicalTable.assimilationPath) match {
        case Some(relativeAp) => relativeAp.heads.exists(_ match {
          case at: AssimilationPath.AssimilationTip => at.tip.assimilation.isIdentifying
          case _ => false
        })
        case None => false
      }
      def isPrimaryKeyColumnGroup(columnGroup: ColumnGroup, logicalTable: LogicalTable): Boolean = (!hasIdentifyingColumnGroup(logicalTable) && columnGroup.isInstanceOf[ColumnGroup.ParentReference]) || isIdentifyingColumnGroup(columnGroup, logicalTable)
      def modifyPrimaryKeys(currentPrimaryKeys: Seq[Column], tableName: String, logicalTable: LogicalTable): Seq[Column] = currentPrimaryKeys ++ (
        if (hasIdentifyingColumnGroup(logicalTable)) Seq()
        else Seq(Column(tableName + suffix, Some(s"Unique identifier for each ${AssimilationPathUtils.name(logicalTable.assimilationPath.commonAssimilationPath)}."), logicalTable.model.config.surrogateKeyColumnType, true, false, logicalTable.assimilationPath, true)))
    }
  }

  trait VersioningPolicy {
    def modifyPrimaryKeys(currentPrimaryKeys: Seq[Column], logicalTable: LogicalTable): Seq[Column]
    def modifyNonPrimaryKeys(currentNonPrimaryKeys: Seq[Column], logicalTable: LogicalTable): Seq[Column]
    def modifyChildForeignKeys(childReference: ColumnGroup.ChildReference, currentForeignKeys: Seq[Column], logicalTable: LogicalTable): Seq[Column]
  }

  object VersioningPolicy {
    case object None extends VersioningPolicy {
      def modifyPrimaryKeys(currentPrimaryKeys: Seq[Column], logicalTable: LogicalTable) = currentPrimaryKeys
      def modifyNonPrimaryKeys(currentNonPrimaryKeys: Seq[Column], logicalTable: LogicalTable) = currentNonPrimaryKeys
      def modifyChildForeignKeys(childReference: ColumnGroup.ChildReference, currentForeignKeys: Seq[Column], logicalTable: LogicalTable) = currentForeignKeys
    }
    case class Type2(validFromColumnName: String = "VALID_FROM", validToColumnName: String = "VALID_TO", dateColumnType: ColumnType = ColumnType("DATE")) extends VersioningPolicy {
      def modifyPrimaryKeys(currentPrimaryKeys: Seq[Column], logicalTable: LogicalTable) = currentPrimaryKeys :+ Column(validFromColumnName, Some("The valid from date."), dateColumnType, true, false, logicalTable.assimilationPath, true)
      def modifyNonPrimaryKeys(currentNonPrimaryKeys: Seq[Column], logicalTable: LogicalTable) = Column(validToColumnName, Some("The valid to date."), dateColumnType, true, true, logicalTable.assimilationPath, false) +: currentNonPrimaryKeys
      def modifyChildForeignKeys(childReference: ColumnGroup.ChildReference, currentForeignKeys: Seq[Column], logicalTable: LogicalTable) = currentForeignKeys.filterNot(_.name == validFromColumnName)
    }
    case class NearestOrientating(revisionDttmSuffix: String = "_REVISION_DTTM", dateColumnType: ColumnType = ColumnType("DATE"), applyToPk: Boolean = true, tieVersions: Boolean = true) extends VersioningPolicy {
      private def revisionDttmColumnName(logicalTable: LogicalTable) = RelationalModel.tableName(logicalTable) + revisionDttmSuffix
      private def revisionDttmColumn(logicalTable: LogicalTable) = Column(revisionDttmColumnName(logicalTable), Some("The revision date / time for this record."), dateColumnType, true, false, logicalTable.assimilationPath, true)
      def modifyPrimaryKeys(currentPrimaryKeys: Seq[Column], logicalTable: LogicalTable) = if (applyToPk) logicalTable.assimilationPath match {
        case dtt: JoinedAssimilationPath.DataTypeTip if dtt.tip.isEffectivelyOrientating => currentPrimaryKeys :+ revisionDttmColumn(logicalTable)
        case jap => currentPrimaryKeys
      }
      else currentPrimaryKeys
      def modifyNonPrimaryKeys(currentNonPrimaryKeys: Seq[Column], logicalTable: LogicalTable) = if (!applyToPk && (logicalTable.assimilationPath match {
        case dtt: JoinedAssimilationPath.DataTypeTip => dtt.tip.isEffectivelyOrientating
        case _ => false
      })) {
        val revisionColumn = revisionDttmColumn(logicalTable)
        val (revisionColumnAdded, columns) = currentNonPrimaryKeys.foldLeft((false, Seq.empty[Column])) {
          case ((encountered, finalColumns), column) => 
            if (!encountered && !column.isIdentifyingRelativeTo(logicalTable.assimilationPath) && finalColumns.last.isIdentifyingRelativeTo(logicalTable.assimilationPath)) (true, finalColumns :+ revisionColumn :+ column)
            else (encountered, finalColumns :+ column)
        }
        if (revisionColumnAdded) columns
        else if (columns.last.isIdentifyingRelativeTo(logicalTable.assimilationPath)) columns :+ revisionColumn
        else revisionColumn +: columns
      } else currentNonPrimaryKeys
      def modifyChildForeignKeys(childReference: ColumnGroup.ChildReference, currentForeignKeys: Seq[Column], logicalTable: LogicalTable) = if (applyToPk && !tieVersions) currentForeignKeys.filterNot(_.foreignKeyReference.exists(fkr => fkr.columnName == fkr.tableName + revisionDttmSuffix)) else currentForeignKeys
    }
  }

  def render(implicit model: Model, config: Config) =
    LogicalRelationalModel(model.effectivelyOrientatingDataTypes.map(dt => toTableFromDataType(JoinedAssimilationPath(dt))).flatMap(_.toSet), config)

  def columnGroupsAndChildTables(assimilationPath: JoinedAssimilationPath.AssimilationTip)(implicit config: Config): (Seq[ColumnGroup], Set[LogicalTableSet]) = {
    val tableSet = toTableFromAssimilation(assimilationPath)
    tableSet.table.assimilationPath match {
      case dtt: JoinedAssimilationPath.DataTypeTip if dtt.effectiveAssimilationStrength == Some(AssimilationStrength.Weak) =>
        (Seq(), Set(tableSet))
      case _ =>
        assimilationPath.tip.assimilation.maximumOccurences match {
          case Some(maxOccurs) =>
            if (maxOccurs > 1 && maxOccurs <= config.maximumOccurencesTableLimit && tableSet.childLogicalTableSets.isEmpty)
              (Seq(ColumnGroup.Repeated(ColumnGroup(tableSet.table), maxOccurs)), Set())
            else if (maxOccurs == 1) (Seq(ColumnGroup(tableSet.table)), tableSet.childLogicalTableSets)
            else (Seq(), Set(tableSet))
          case None =>
            (Seq(), Set(tableSet))
        }
    }
  }

  def mergeJoinableChildLogicalTableSets(parent: LogicalTable, childLogicalTableSets: Set[LogicalTableSet])(implicit config: Config): Set[LogicalTableSet] = {
    def recurse(mergedCts: Set[LogicalTableSet], remainingCts: Set[LogicalTableSet]): Set[LogicalTableSet] = {
      if (remainingCts.isEmpty) mergedCts
      else {
        val (headSet, tailSet) = remainingCts.splitAt(1)
        val (mergeable, notMergeable) = tailSet.partition(_.canMergeWith(parent, headSet.head))
        val newMergedCts = mergedCts + mergeable.foldLeft(headSet.head)((merged, next) => merged.mergeWith(parent, next))
        recurse(newMergedCts, notMergeable)
      }
    }
    recurse(Set(), childLogicalTableSets)
  }

  def toSimpleColumnGroup(assimilationPath: JoinedAssimilationPath.DataTypeTip)(implicit config: Config): ColumnGroup.SimpleColumn =
    ColumnGroup.SimpleColumn(assimilationPath, config.mappedColumnType(assimilationPath.tip).get)

  def toTableFromAssimilation(assimilationPath: JoinedAssimilationPath.AssimilationTip)(implicit config: Config): LogicalTableSet = {
    if (assimilationPath.tip.dataTypeReferences.size == 1) toTableFromDataTypeOrReference(assimilationPath + assimilationPath.tip.dataTypes.head)
    else if (assimilationPath.tip.dataTypeReferences.size > 1) {
      val (columns, childTables) = assimilationPath.tip.dataTypes.map(dt => toTableFromDataTypeOrReference(assimilationPath + dt)).foldLeft((Seq.empty[ColumnGroup], Set.empty[LogicalTableSet])) {
        case ((fcs, fts), ts) =>
          if (ts.table.assimilationPath.asInstanceOf[JoinedAssimilationPath.DataTypeTip].effectiveAssimilationStrength == Some(AssimilationStrength.Weak)) (fcs, fts + ts)
          else if (!config.maximumSubTypeColumnsBeforeSplittingOut.isDefined || ts.table.columnGroups.filterNot(_.isInstanceOf[ColumnGroup.ParentReference]).size <= config.maximumSubTypeColumnsBeforeSplittingOut.get) {
            (fcs :+ ColumnGroup(ts.table), fts ++ ts.childLogicalTableSets)
          } else (fcs, fts + ts)
      }
      val table = LogicalTable(
        assimilationPath = assimilationPath,
        columnGroups = Seq(ColumnGroup.ParentReference(assimilationPath)) ++ columns :+ ColumnGroup.EnumerationColumn(assimilationPath))
      LogicalTableSet(table, mergeJoinableChildLogicalTableSets(table, childTables))
    } else throw new IllegalStateException(s"It is illegal for assimililation '${assimilationPath.tip}' to have no data type references.")
  }

  def toTableFromDataTypeOrReference(assimilationPath: JoinedAssimilationPath.DataTypeTip)(implicit config: Config): LogicalTableSet = {
    if (assimilationPath.effectiveAssimilationStrength == Some(AssimilationStrength.Reference)) {
      LogicalTableSet(
        LogicalTable(
          assimilationPath = assimilationPath,
          columnGroups = Seq(ColumnGroup.ParentReference(assimilationPath), ColumnGroup.ChildReference(assimilationPath))), Set())
    } else if (config.mappedColumnType(assimilationPath.tip).isDefined) {
      LogicalTableSet(
        LogicalTable(
          assimilationPath = assimilationPath,
          columnGroups = Seq(ColumnGroup.ParentReference(assimilationPath), toSimpleColumnGroup(assimilationPath))), Set())
    } else toTableFromDataType(assimilationPath)
  }

  def toTableFromDataType(assimilationPath: JoinedAssimilationPath.DataTypeTip)(implicit config: Config): LogicalTableSet = {
    val (columnGroups, childLogicalTableSets) = assimilationPath.tip.assimilations.map(a => columnGroupsAndChildTables(assimilationPath + a)).foldLeft(Seq.empty[ColumnGroup], Set.empty[LogicalTableSet]) {
      case ((fcgs, fcts), (cgs, cts)) => (fcgs ++ cgs, fcts ++ cts)
    }
    val table = LogicalTable(
      assimilationPath = assimilationPath,
      columnGroups = (if (assimilationPath.parents.isEmpty) Seq() else Seq(ColumnGroup.ParentReference(assimilationPath))) ++ columnGroups)
    LogicalTableSet(table, mergeJoinableChildLogicalTableSets(table, childLogicalTableSets))
  }

  def relativeName(parentJap: JoinedAssimilationPath, childJap: JoinedAssimilationPath): String =
    childJap.relativeTo(parentJap).map(_.commonAssimilationPath) match {
      case Some(ap) => AssimilationPathUtils.name(ap)
      case None =>
        AssimilationPathUtils.name {
          val commonPathSeq = childJap.commonAssimilationPath.toSeq
          if (commonPathSeq.size > 1) commonPathSeq.drop(commonPathSeq.size - 2).head.withNoParent
          else childJap.commonAssimilationPath.withNoParent
        }
    }
  def relativeDescription(parentJap: JoinedAssimilationPath, childJap: JoinedAssimilationPath): String =
    childJap.relativeTo(parentJap).map(_.commonAssimilationPath) match {
      case Some(ap) => AssimilationPathUtils.description(ap)
      case None =>
        AssimilationPathUtils.description {
          val commonPathSeq = childJap.commonAssimilationPath.toSeq
          if (commonPathSeq.size > 1) commonPathSeq.drop(commonPathSeq.size - 2).head.withNoParent
          else childJap.commonAssimilationPath.withNoParent
        }
    }
}