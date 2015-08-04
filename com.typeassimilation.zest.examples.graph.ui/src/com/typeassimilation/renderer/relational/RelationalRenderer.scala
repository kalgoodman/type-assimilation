package com.typeassimilation.renderer.relational

import scala.collection.mutable
import com.typeassimilation.model._

object RelationalRenderer {
  sealed trait ColumnGroup {
    type T <: ColumnGroup
    def assimilationPath: JoinedAssimilationPath[_]
    def technicalDescription(logicalTable: LogicalTable, prefix: String = "")(implicit model: Model) : String
    def size: Int
    def canMergeWith(that: ColumnGroup): Boolean = this.getClass == that.getClass && assimilationPath.tip == that.assimilationPath.tip
    def mergeWith(that: ColumnGroup): ColumnGroup = if (canMergeWith(that)) doMerge(this.asInstanceOf[T], that.asInstanceOf[T]) else throw new IllegalStateException(s"Cannot merge ColumnGroup '$this' with '$that'.")
    protected def doMerge(thisColumnGroup: T, thatColumnGroup: T): T
  }
  object ColumnGroup {
    case class Repeated(columnGroup: ColumnGroup, repeats: Int) extends ColumnGroup {
      type T = Repeated
      def assimilationPath = columnGroup.assimilationPath
      def technicalDescription(logicalTable: LogicalTable, prefix: String)(implicit model: Model) : String = s"$prefix${repeats} x { ${columnGroup.technicalDescription(logicalTable)} }"
      def doMerge(thisColumnGroup: Repeated, thatColumnGroup: Repeated): Repeated = thisColumnGroup.copy(columnGroup = columnGroup.mergeWith(thatColumnGroup.columnGroup))
      def size = columnGroup.size * repeats
    }
    case class NestedTable(assimilationPath: JoinedAssimilationPath[_], columnGroups: Seq[ColumnGroup]) extends ColumnGroup {
      type T = NestedTable
      def technicalDescription(logicalTable: LogicalTable, prefix: String)(implicit model: Model) : String = s"${prefix}NESTED: ${AssimilationPathUtils.relativeName(logicalTable.assimilationPath, assimilationPath)} [$assimilationPath]" + (if (columnGroups.isEmpty) "" else s" {\n${columnGroups.map(_.technicalDescription(logicalTable, prefix + "  ")).mkString(s"\n")}\n$prefix}")
      def doMerge(thisColumnGroup: NestedTable, thatColumnGroup: NestedTable): NestedTable = thisColumnGroup.copy(assimilationPath = thisColumnGroup.assimilationPath + thatColumnGroup.assimilationPath, columnGroups = (thisColumnGroup.columnGroups zip thatColumnGroup.columnGroups).map {
        case (thisCg, thatCg) => thisCg.mergeWith(thatCg)  
      })
      def size = columnGroups.foldLeft(0)((sum, cg) => sum + cg.size)
    }
    case class SimpleColumn(assimilationPath: JoinedAssimilationPath[_], columnType: ColumnType) extends ColumnGroup {
      type T = SimpleColumn
      def technicalDescription(logicalTable: LogicalTable, prefix: String)(implicit model: Model) : String = s"$prefix${AssimilationPathUtils.relativeName(logicalTable.assimilationPath, assimilationPath)} $columnType [$assimilationPath]"
      def doMerge(thisColumnGroup: SimpleColumn, thatColumnGroup: SimpleColumn): SimpleColumn = thisColumnGroup.copy(assimilationPath = thisColumnGroup.assimilationPath + thatColumnGroup.assimilationPath)
      def size = 1
    }
    case class EnumerationColumn(assimilationPath: JoinedAssimilationPath[_]) extends ColumnGroup {
      type T = EnumerationColumn
      def technicalDescription(logicalTable: LogicalTable, prefix: String)(implicit model: Model) : String = s"${prefix}ENUMERATION: ${AssimilationPathUtils.relativeName(logicalTable.assimilationPath, assimilationPath)} [$assimilationPath]" 
      def doMerge(thisColumnGroup: EnumerationColumn, thatColumnGroup: EnumerationColumn): EnumerationColumn = thisColumnGroup.copy(assimilationPath = thisColumnGroup.assimilationPath + thatColumnGroup.assimilationPath)
      def size = 1
    }
    case class ParentReference(assimilationPath: JoinedAssimilationPath[_]) extends ColumnGroup {
      type T = ParentReference
      def technicalDescription(logicalTable: LogicalTable, prefix: String)(implicit model: Model): String = s"${prefix}Parent reference to ${AssimilationPathUtils.absoluteName(assimilationPath)} [$assimilationPath]" 
      def doMerge(thisColumnGroup: ParentReference, thatColumnGroup: ParentReference): ParentReference = thisColumnGroup.copy(assimilationPath = thisColumnGroup.assimilationPath + thatColumnGroup.assimilationPath)
      def size = 1
      def parentAssimilationPath = {
        val directParent = assimilationPath.parents.head
        directParent.tip match {
          case Left(dataType) => directParent
          case Right(assimilation) => directParent.parents.head
        }
      }
    }
    case class ChildReference(assimilationPath: JoinedAssimilationPath[_]) extends ColumnGroup {
      type T = ChildReference
      def technicalDescription(logicalTable: LogicalTable, prefix: String)(implicit model: Model): String = s"${prefix}Child reference to ${AssimilationPathUtils.absoluteName(assimilationPath)} [$assimilationPath]"
      def doMerge(thisColumnGroup: ChildReference, thatColumnGroup: ChildReference): ChildReference = thisColumnGroup.copy(assimilationPath = thisColumnGroup.assimilationPath + thatColumnGroup.assimilationPath)
      def size = 1
      def childAssimilationPath(implicit model: Model) = assimilationPath.tipDataTypeOption match {
        case Some(dataType) if dataType.isOrientating => JoinedAssimilationPath(dataType)
        case None => throw new IllegalStateException(s"Currently can only deal with child references to orientating data types - not [$assimilationPath]")
      }
    }
    def apply(logicalTable: LogicalTable): ColumnGroup = {
      val columnGroups = logicalTable.columnGroups.filterNot(_.isInstanceOf[ParentReference])
      if (columnGroups.size == 1 && columnGroups.head.assimilationPath == logicalTable.assimilationPath) columnGroups.head
      else NestedTable(logicalTable.assimilationPath, columnGroups)
    }
  }
  case class LogicalTable(assimilationPath: JoinedAssimilationPath[_], columnGroups: Seq[ColumnGroup]) {
    import ColumnGroup._
    def parentReferenceOption = columnGroups.flatMap { case par: ColumnGroup.ParentReference => Some(par); case _ => None }.headOption
    def technicalDescription(implicit model: Model) = s"TABLE: ${AssimilationPathUtils.absoluteName(assimilationPath)} [${assimilationPath}] {\n${columnGroups.map(_.technicalDescription(this, "  ")).mkString("\n")}\n}\n"
    def allColumnGroups: Seq[ColumnGroup] = {
      def recurseColumnGroups(cg: ColumnGroup): Seq[ColumnGroup] = cg match {
        case r:Repeated => Seq(r) ++ recurseColumnGroups(r.columnGroup)
        case nt:NestedTable => Seq(nt) ++ nt.columnGroups.flatMap(recurseColumnGroups(_))
        case cg => Seq(cg)
      }
      columnGroups.flatMap(recurseColumnGroups(_))
    }
    def nestedAssimilationPaths = allColumnGroups.flatMap { case nt: NestedTable => Some(nt); case _ => None }.map(_.assimilationPath).toSet
    def assimilationPaths = nestedAssimilationPaths ++ AssimilationPathUtils.merge(nestedAssimilationPaths) + assimilationPath 
    def canMergeWith(parent: LogicalTable, that: LogicalTable): Boolean =
      this.assimilationPath.tip == that.assimilationPath.tip &&
      parent.assimilationPaths.contains(this.parentReferenceOption.get.parentAssimilationPath) &&
      parent.assimilationPaths.contains(that.parentReferenceOption.get.parentAssimilationPath)
    def mergeWith(parent: LogicalTable, that: LogicalTable): LogicalTable = if (canMergeWith(parent, that)) copy(assimilationPath = this.assimilationPath + that.assimilationPath, columnGroups = (this.columnGroups zip that.columnGroups).map {
      case (thisCg, thatCg) => thisCg.mergeWith(thatCg)
    }) else throw new IllegalStateException(s"Cannot merge $this with $that.")
  }
  case class LogicalRelationalModel(tables: Set[LogicalTable], model: Model, config: Config) {
    private val assimilationPathToTableMap: Map[JoinedAssimilationPath[_], LogicalTable] = tables.flatMap(lt => lt.assimilationPaths.map(ap => ap.asInstanceOf[JoinedAssimilationPath[Nothing]] -> lt)).toMap
    def follow(parentReference: ColumnGroup.ParentReference) = assimilationPathToTableMap(parentReference.parentAssimilationPath)
    def follow(childReference: ColumnGroup.ChildReference)(implicit model: Model) = assimilationPathToTableMap(childReference.childAssimilationPath)
    override def toString = tables.map(_.technicalDescription(model)).mkString("\n")
  }
  
  case class LogicalTableSet(table: LogicalTable, childLogicalTableSets: Set[LogicalTableSet]) {
    def toSet: Set[LogicalTable] = childLogicalTableSets.flatMap(_.toSet) + table
    def canMergeWith(parent: LogicalTable, that: LogicalTableSet) = table.canMergeWith(parent, that.table)
    def mergeWith(parent: LogicalTable, that: LogicalTableSet)(implicit config: Config, model: Model): LogicalTableSet = {
      def recurse(p: LogicalTable, ts1: LogicalTableSet, ts2: LogicalTableSet): LogicalTableSet = {
        LogicalTableSet(
          table = ts1.table.mergeWith(p, ts2.table),
          childLogicalTableSets = {
            val cts2Map = ts2.childLogicalTableSets.map(cts2 => cts2.table.assimilationPath.asInstanceOf[JoinedAssimilationPath[Nothing]] -> cts2).toMap
            def failSelectingEquivalentChild(cts1: LogicalTableSet) = throw new IllegalStateException(s"Could not find equivalent child to ${cts1.table.technicalDescription} in ${ts2.childLogicalTableSets.map(_.table.technicalDescription).mkString(",")}.")
            ts1.childLogicalTableSets.map{cts1 => 
              val ts2EquivalentAp = AssimilationPathUtils.mostJoinable[Nothing](cts1.table.assimilationPath, cts2Map.keys).headOption.getOrElse(failSelectingEquivalentChild(cts1))
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
    identifierLength: Int = 30,
    maximumSubTypeColumnsBeforeSplittingOut: Option[Int] = Some(10),
    namingPolicy: NamingPolicy = NamingPolicy.Default,
    primaryKeyPolicy: PrimaryKeyPolicy = PrimaryKeyPolicy.SurrogateKeyGeneration)
    
  trait NamingPolicy {
    def tableName(logicalTable: LogicalTable)(implicit config: Config, model: Model): String
    def tableDescription(logicalTable: LogicalTable)(implicit config: Config, model: Model): Option[String]
    def columnName(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath[_], repeatIndexes: Seq[Int])(implicit config: Config, model: Model): String
    def columnDescription(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath[_], repeatIndexes: Seq[Int])(implicit config: Config, model: Model): Option[String]
    def foreignKeyColumnName(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath[_], referencePrimaryKeyColumn: Column, repeatIndexes: Seq[Int])(implicit config: Config, model: Model): String
    def foreignKeyColumnDescription(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath[_], referencePrimaryKeyColumn: Column, repeatIndexes: Seq[Int])(implicit config: Config, model: Model): Option[String]
    def parentForeignKeyColumnName(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath[_], referencePrimaryKeyColumn: Column)(implicit config: Config, model: Model): String
    def parentForeignKeyColumnDescription(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath[_], referencePrimaryKeyColumn: Column)(implicit config: Config, model: Model): Option[String]
    def enumerationName(assimilationPath: JoinedAssimilationPath[_])(implicit config: Config, model: Model): String
    def enumerationValueName(assimilationPath: JoinedAssimilationPath[_])(implicit config: Config, model: Model): String
    def enumerationValueDescription(assimilationPath: JoinedAssimilationPath[_])(implicit config: Config, model: Model): Option[String]
  }

  object NamingPolicy {
    object Default extends NamingPolicy {
      val shorten = Map(
          "IDENTIFIER" -> "ID",
          "NAME" -> "NM",
          "DATE" -> "DT",
          "DATETIME" -> "DTTM",
          "TIME" -> "TM",
          "NUMBER" -> "NO",
          "COUNT" -> "CNT"
        ).withDefault(s => s)
      private def underscoreDelimitedName(s: String) = mutable.LinkedHashSet(s.trim.replaceAll("\\s+", "_").toUpperCase.split('_'): _*).map(shorten).mkString("_")
      def tableName(logicalTable: LogicalTable)(implicit config: Config, model: Model): String = underscoreDelimitedName(AssimilationPathUtils.absoluteName(logicalTable.assimilationPath))
      def tableDescription(logicalTable: LogicalTable)(implicit config: Config, model: Model): Option[String] = logicalTable.assimilationPath.tipDescription
      def columnName(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath[_], repeatIndexes: Seq[Int])(implicit config: Config, model: Model): String = underscoreDelimitedName(AssimilationPathUtils.relativeName(parentLogicalTable.assimilationPath, assimilationPath)) + repeatIndexes.map("_" + _).mkString("_")
      def columnDescription(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath[_], repeatIndexes: Seq[Int])(implicit config: Config, model: Model): Option[String] = assimilationPath.tipDescription
      def foreignKeyColumnName(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath[_], referencePrimaryKeyColumn: Column, repeatIndexes: Seq[Int])(implicit config: Config, model: Model): String = {
        val namesInFirstOccurenceOrder = mutable.LinkedHashSet(columnName(parentLogicalTable, assimilationPath, repeatIndexes).split('_'):_*)
        namesInFirstOccurenceOrder ++= referencePrimaryKeyColumn.name.split('_')
        namesInFirstOccurenceOrder.mkString("_") + repeatIndexes.map("_" + _).mkString("_")
      }
      def foreignKeyColumnDescription(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath[_], referencePrimaryKeyColumn: Column, repeatIndexes: Seq[Int])(implicit config: Config, model: Model): Option[String] = assimilationPath.tipDescription 
      def parentForeignKeyColumnName(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath[_], referencePrimaryKeyColumn: Column)(implicit config: Config, model: Model): String = referencePrimaryKeyColumn.name
      def parentForeignKeyColumnDescription(parentLogicalTable: LogicalTable, assimilationPath: JoinedAssimilationPath[_], referencePrimaryKeyColumn: Column)(implicit config: Config, model: Model): Option[String] = referencePrimaryKeyColumn.description.map("Reference to " + _)
      def enumerationName(assimilationPath: JoinedAssimilationPath[_])(implicit config: Config, model: Model): String = assimilationPath.tipAssimilation.assimilation.name.getOrElse(assimilationPath.tipName).replaceAll("\\s+", "")
      def enumerationValueName(assimilationPath: JoinedAssimilationPath[_])(implicit config: Config, model: Model): String = underscoreDelimitedName(assimilationPath.tipDataType.name)
      def enumerationValueDescription(assimilationPath: JoinedAssimilationPath[_])(implicit config: Config, model: Model): Option[String] = assimilationPath.tipDescription 
    }
  }
  
  trait PrimaryKeyPolicy {
    def isPrimaryKeyColumnGroup(columnGroup: ColumnGroup, logicalTable: LogicalTable, logicalRelationalModel: LogicalRelationalModel): Boolean
    def modifyPrimaryKeys(currentPrimaryKeys: Seq[Column], tableName: String, logicalTable: LogicalTable, logicalRelationalModel: LogicalRelationalModel): Seq[Column]
  }

  object PrimaryKeyPolicy {
    object SurrogateKeyGeneration extends PrimaryKeyPolicy {
      import ColumnGroup._
      private def isManyToMany(logicalTable: LogicalTable) = logicalTable.parentReferenceOption.flatMap(pr => logicalTable.columnGroups.find {
        case cr: ChildReference => cr.assimilationPath == pr.assimilationPath
        case _ => false
      }).isDefined
      def isPrimaryKeyColumnGroup(columnGroup: ColumnGroup, logicalTable: LogicalTable, logicalRelationalModel: LogicalRelationalModel): Boolean = isManyToMany(logicalTable) && (columnGroup match {
        case pr: ParentReference => true
        case cr: ChildReference => cr.assimilationPath == logicalTable.parentReferenceOption.get.assimilationPath 
        case _ => false
      })
      def modifyPrimaryKeys(currentPrimaryKeys: Seq[Column], tableName: String, logicalTable: LogicalTable, logicalRelationalModel: LogicalRelationalModel): Seq[Column] = if (currentPrimaryKeys.isEmpty) Seq(Column(tableName + "_SK", Some(s"Surrogate Key (system generated) for the ${tableName} table."), ColumnType.Number(15, 0), true, false, logicalTable.assimilationPath, true)) else currentPrimaryKeys
    }
    case class NaturalKey(suffix: String = "_ID") extends PrimaryKeyPolicy {
      private def hasIdentifyingColumnGroup(logicalTable: LogicalTable) = logicalTable.columnGroups.find(_.assimilationPath.tipAssimilation.assimilation.isIdentifying).isDefined
      def isPrimaryKeyColumnGroup(columnGroup: ColumnGroup, logicalTable: LogicalTable, logicalRelationalModel: LogicalRelationalModel): Boolean = (!hasIdentifyingColumnGroup(logicalTable) && columnGroup.isInstanceOf[ColumnGroup.ParentReference]) || columnGroup.assimilationPath.tipAssimilation.assimilation.isIdentifying
      def modifyPrimaryKeys(currentPrimaryKeys: Seq[Column], tableName: String, logicalTable: LogicalTable, logicalRelationalModel: LogicalRelationalModel): Seq[Column] = currentPrimaryKeys ++ (
          if (hasIdentifyingColumnGroup(logicalTable)) Seq()
          else Seq(Column(tableName + suffix, Some(s"Unique identifier for each ${AssimilationPathUtils.absoluteName(logicalTable.assimilationPath)(logicalRelationalModel.model)}."), ColumnType.Number(15, 0), true, false, logicalTable.assimilationPath, true))
      )
    }
  }
  
  def render(implicit model: Model, config: Config = Config()) = 
      LogicalRelationalModel(model.orientatingDataTypes.map(dt => toTableFromDataType(JoinedAssimilationPath(dt))).flatMap(_.toSet), model, config)

  def toPrimitiveColumnGroup(assimilationPath: JoinedAssimilationPath[Assimilation])(implicit config: Config, model: Model): ColumnGroup = {
    def columnGroup(columnType: ColumnType) = ColumnGroup.SimpleColumn(assimilationPath, columnType)
    import Preset.DataType._
    assimilationPath.singleTipDataType match {
      case Identifier => columnGroup(ColumnType.VariableCharacter(config.identifierLength))
      case DateTime => columnGroup(ColumnType.DateTime)
      case Date => columnGroup(ColumnType.Date)
      case IntegralNumber => columnGroup(ColumnType.Integer)
      case DecimalNumber => columnGroup(ColumnType.Number(10, 5))
      case Boolean => columnGroup(ColumnType.FixedCharacter(1))
      case Code2 => columnGroup(ColumnType.FixedCharacter(2))
      case Code3 => columnGroup(ColumnType.FixedCharacter(3))
      case TextBlock => columnGroup(ColumnType.VariableCharacter(500))
      case ShortName => columnGroup(ColumnType.VariableCharacter(50))
      case LongName => columnGroup(ColumnType.VariableCharacter(100))
      case x => throw new IllegalArgumentException(s"${x.filePath} is not accounted for...")
    }
  }

  def columnGroupsAndChildTables(assimilationPath: JoinedAssimilationPath[Assimilation])(implicit config: Config, model: Model): (Seq[ColumnGroup], Set[LogicalTableSet]) = {
    val tableSet = toTableFromAssimilation(assimilationPath)
    assimilationPath.tipAssimilation.assimilation.maximumOccurences match {
      case Some(maxOccurs) =>
        if (maxOccurs > 1 && maxOccurs <= config.maximumOccurencesTableLimit && tableSet.childLogicalTableSets.isEmpty)
          (Seq(ColumnGroup.Repeated(ColumnGroup(tableSet.table), maxOccurs)), Set())
        else if (maxOccurs == 1) (Seq(ColumnGroup(tableSet.table)), tableSet.childLogicalTableSets)
        else (Seq(), Set(tableSet))
      case None =>
        (Seq(), Set(tableSet))
    }
  }

  def mergeJoinableChildLogicalTableSets(parent: LogicalTable, childLogicalTableSets: Set[LogicalTableSet])(implicit config: Config, model: Model): Set[LogicalTableSet] = {
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

  def toTableFromAssimilation(assimilationPath: JoinedAssimilationPath[Assimilation])(implicit config: Config, model: Model): LogicalTableSet = {
    if (assimilationPath.tipAssimilation.dataTypeFilePaths.size == 1 && assimilationPath.singleTipDataType.primitive) {
      LogicalTableSet(LogicalTable(
          assimilationPath = assimilationPath,
          columnGroups = Seq(ColumnGroup.ParentReference(assimilationPath), toPrimitiveColumnGroup(assimilationPath)))
        , Set())
    }
    else if (assimilationPath.tipDataTypes.size == 1) toTableFromDataTypeOrReference(assimilationPath + assimilationPath.singleTipDataType)
    else if (assimilationPath.tipDataTypes.size > 1) {
      val (columns, childTables) = assimilationPath.tipDataTypes.map(dt => toTableFromDataTypeOrReference(assimilationPath + dt)).foldLeft((Seq.empty[ColumnGroup], Set.empty[LogicalTableSet])) {
        case ((fcs, fts), ts) =>
          if (!config.maximumSubTypeColumnsBeforeSplittingOut.isDefined || ts.table.columnGroups.filterNot(_.isInstanceOf[ColumnGroup.ParentReference]).size <= config.maximumSubTypeColumnsBeforeSplittingOut.get) {
            (fcs :+ ColumnGroup(ts.table), fts ++ ts.childLogicalTableSets)
          } else (fcs, fts + ts)
      }
      val table = LogicalTable(
          assimilationPath = assimilationPath,
          columnGroups = Seq(ColumnGroup.ParentReference(assimilationPath)) ++ columns :+ ColumnGroup.EnumerationColumn(assimilationPath)
        )
      LogicalTableSet(table, mergeJoinableChildLogicalTableSets(table, childTables))
    } else throw new IllegalStateException(s"It is illegal for assimililation '${assimilationPath.tipAssimilation}' to have no data type references.")
  }

  def toTableFromDataTypeOrReference(assimilationPath: JoinedAssimilationPath[DataType])(implicit config: Config, model: Model): LogicalTableSet = {
    if (assimilationPath.tipDataType.isOrientating) {
      LogicalTableSet(
          LogicalTable(
            assimilationPath = assimilationPath,
            columnGroups = Seq(ColumnGroup.ParentReference(assimilationPath), ColumnGroup.ChildReference(assimilationPath))
          ),
          Set.empty[LogicalTableSet])
    } else
      toTableFromDataType(assimilationPath)
  }

  def toTableFromDataType(assimilationPath: JoinedAssimilationPath[DataType])(implicit config: Config, model: Model): LogicalTableSet = {
    val (columnGroups, childLogicalTableSets) = assimilationPath.tipDataType.assimilations.map(a => columnGroupsAndChildTables(assimilationPath + a)).foldLeft(Seq.empty[ColumnGroup], Set.empty[LogicalTableSet]) {
      case ((fcgs, fcts), (cgs, cts)) => (fcgs ++ cgs, fcts ++ cts)
    }
    val table = LogicalTable(
        assimilationPath = assimilationPath,
        columnGroups = (if (assimilationPath.parents.isEmpty) Seq() else Seq(ColumnGroup.ParentReference(assimilationPath))) ++ columnGroups
      )
    LogicalTableSet(table, mergeJoinableChildLogicalTableSets(table, childLogicalTableSets))
  }

}