package com.typeassimilation.model

import Model.Implicits._
import scala.collection.mutable

object RelationalRenderer {
  sealed trait ColumnType
  object ColumnType {
    case object Date extends ColumnType
    case object Time extends ColumnType
    case object DateTime extends ColumnType
    case class VariableCharacter(size: Int) extends ColumnType
    case class FixedCharacter(size: Int) extends ColumnType
    case object CharacterLob extends ColumnType
    case class Number(depth: Int, precision: Int) extends ColumnType
    case object Integer extends ColumnType
  }
  case class RelationalModel(tables: Set[Table]) {
    private val nameToTableMap = tables.map(t => t.name -> t).toMap
    def table(name: String) = nameToTableMap.get(name)
    def enumerations: Set[Enumeration] = for {
      t <- tables
      c <- t.columns
      e <- c.enumeration
    } yield e
    override def toString = "TABLES:\n\n" + tables.toList.sortBy(_.name).mkString("\n") +
      "\nENUMERATIONS:\n\n" + enumerations.toList.sortBy(_.name).mkString("\n")
  }
  case class TableSet(table: Table, childTableSets: Set[TableSet]) {
    def toSet: Set[Table] = childTableSets.flatMap(_.toSet) + table
    def isMergeableWith(other: TableSet) = table.foreignKeys == other.table.foreignKeys &&
      table.assimilationPath.tip == other.table.assimilationPath.tip &&
      table.assimilationPath.parents.size == 1 && other.table.assimilationPath.parents.size == 1 &&
      table.assimilationPath.parents.head.tip == other.table.assimilationPath.parents.head.tip
    def mergeWith(other: TableSet)(implicit config: Config, model: Model): TableSet = {
      def recurse(parentForeignKeyColumns: Seq[Column], ts1: TableSet, ts2: TableSet): TableSet = {
        ts1.table.validate
        ts2.table.validate
        val mergedTableInformation = TableInformation(ts1.table.assimilationPath + ts2.table.assimilationPath)
        def mergeColumns(c1: Column, c2: Column) = {
          val mergedColumnAssimilationPath = c1.assimilationPath + c2.assimilationPath
          c1.copy(
            name = config.namingPolicy.toColumnName(mergedTableInformation.assimilationPath, mergedColumnAssimilationPath, c1.repeatIndex),
            description = config.namingPolicy.toColumnDescription(mergedTableInformation.assimilationPath, mergedColumnAssimilationPath),
            assimilationPath = mergedColumnAssimilationPath,
            enumeration = c1.enumeration.map(e1 => toEnumeration(e1.assimilationPath + c2.enumeration.get.assimilationPath)))
        }
        TableSet(
          table = {
            Table(
              name = config.namingPolicy.toTableName(mergedTableInformation.assimilationPath),
              description = config.namingPolicy.toTableDescription(mergedTableInformation.assimilationPath),
              // FIXME Currently ignores the possibility of foreign keys to not the parent
              columns =
                mergedTableInformation.primaryKeys ++
                  parentForeignKeyColumns ++
                  ts1.table.nonKeyColumns.map(c => mergeColumns(c, ts2.table.column(c.name).get)),
              assimilationPath = mergedTableInformation.assimilationPath)
          },
          childTableSets = {
            val cts2Map = ts2.childTableSets.map(cts2 => cts2.table.name -> cts2).toMap
            ts1.childTableSets.map(cts1 => recurse(mergedTableInformation.foreignKeys, cts1, cts2Map(cts1.table.name)))
          })
      }
      recurse(table.foreignKeys, this, other)
    }
    def migrateHeadColumnsTo(newParent: TableInformation)(implicit config: Config, model: Model): (Seq[Column], Set[TableSet]) = (
      table.columns.filter(c => !c.isPrimaryKey && !c.foreignKeyReference.isDefined),
      childTableSets.map(ts => ts.copy(table = ts.table.copy(columns = {
        val columnsWithRemovedPreviousParent = ts.table.columns.filterNot(c => c.foreignKeyReference.isDefined && c.foreignKeyReference.get.tableName == table.name)
        val keys = columnsWithRemovedPreviousParent.filter(_.isPrimaryKey) match {
          case pks if pks.isEmpty => newParent.primaryForeignKeys
          case pks => pks ++ newParent.foreignKeys
        }
        keys ++ columnsWithRemovedPreviousParent.filter(!_.isPrimaryKey)
      }))))
  }
  case class Table(name: String, description: Option[String], columns: Seq[Column], assimilationPath: JoinedAssimilationPath[_]) {
    private val nameToColumnMap = columns.map(c => c.name -> c).toMap
    def column(name: String) = nameToColumnMap.get(name)
    def primaryKeys = columns.filter(_.isPrimaryKey)
    def foreignKeys = columns.filter(_.foreignKeyReference.isDefined)
    def nonKeyColumns = columns.filter(c => !c.isPrimaryKey && !c.foreignKeyReference.isDefined)
    def validate = {
      val repeatedColumns = columns.groupBy(_.name).mapValues(_.size).filter {
        case (name, size) => size > 1
      }
      if (!repeatedColumns.isEmpty) throw new IllegalStateException(s"Table contains repeated column names: $this")
    }
    override def toString = name + (if (description.isDefined) s": ${description.get}" else "") + s" [$assimilationPath] {\n" + columns.map(c => s"\t${c.toString}").mkString("\n") + "\n}\n"
  }
  case class Column(name: String, description: Option[String], `type`: ColumnType, assimilationPath: JoinedAssimilationPath[_], isPrimaryKey: Boolean = false, enumeration: Option[Enumeration] = None, foreignKeyReference: Option[ColumnReference] = None, repeatIndex: Option[Int] = None) {
    override def toString = s"$name " + `type`.toString + (if (enumeration.isDefined) s" (ENUM: ${enumeration.get.name})" else "") + {
      if (isPrimaryKey && foreignKeyReference.isEmpty) "(PK)"
      else if (!isPrimaryKey && foreignKeyReference.isDefined) "(FK -> " + foreignKeyReference.get + ")"
      else if (isPrimaryKey && foreignKeyReference.isDefined) "(PFK -> " + foreignKeyReference.get + ")"
      else ""
    } + (if (description.isDefined) s": ${description.get}" else "") + s" [$assimilationPath]"
    def toForeignKeyReference(tableName: String) = ColumnReference(tableName, name)
  }
  case class ColumnReference(tableName: String, columnName: String) {
    override def toString = s"$tableName.$columnName"
  }
  case class Enumeration(name: String, values: Seq[EnumerationValue], assimilationPath: JoinedAssimilationPath[AssimilationReference]) {
    def maximumValueSize = values.map(_.name.length).foldLeft(0)((size, v) => if (v > size) v else size)
    override def toString = s"$name [$assimilationPath] {\n" + values.map(v => s"\t${v.toString}").mkString("\n") + "\n}\n"
  }
  case class EnumerationValue(name: String, description: Option[String], assimilationPath: JoinedAssimilationPath[DataType]) {
    override def toString = name + (if (description.isDefined) s" - ${description.get}" else "") + s"  [$assimilationPath]"
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
    moneyAmountSuffix: String = "_AMT",
    moneyCurrencyCodeSuffix: String = "_CCY_CD",
    maximumSubTypeColumnsBeforeSplittingOut: Option[Int] = Some(4),
    namingPolicy: NamingPolicy = NamingPolicy.Default)

  trait NamingPolicy {
    def toTableName(assimilationPath: JoinedAssimilationPath[_])(implicit config: Config, model: Model): String
    def toTableDescription(assimilationPath: JoinedAssimilationPath[_])(implicit config: Config, model: Model): Option[String]
    def toForeignKeyDescription(primaryKeyColumn: Column)(implicit config: Config, model: Model): String
    def toColumnName(tableAssimilationPath: JoinedAssimilationPath[_], columnAssimilationPath: JoinedAssimilationPath[_], repeatIndex: Option[Int] = None, enumeration: Option[Enumeration] = None)(implicit config: Config, model: Model): String
    def toColumnDescription(tableAssimilationPath: JoinedAssimilationPath[_], columnAssimilationPath: JoinedAssimilationPath[_])(implicit config: Config, model: Model): Option[String]
    def toEnumerationName(assimilationPath: JoinedAssimilationPath[AssimilationReference])(implicit config: Config, model: Model): String
    def toEnumerationDesciption(assimilationPath: JoinedAssimilationPath[AssimilationReference])(implicit config: Config, model: Model): Option[String]
    def toEnumerationValueName(assimilationPath: JoinedAssimilationPath[DataType])(implicit config: Config, model: Model): String
    def toEnumerationValueDescription(assimilationPath: JoinedAssimilationPath[DataType])(implicit config: Config, model: Model): Option[String]
  }

  object NamingPolicy {
    object Default extends NamingPolicy {
      private def cleanAndUpperCase(s: String) = mutable.LinkedHashSet(s.trim.replaceAll("\\s+", "_").toUpperCase.split('_'): _*).mkString("_")
      def toTableName(assimilationPath: JoinedAssimilationPath[_])(implicit config: Config, model: Model): String = cleanAndUpperCase(assimilationPath.tipName)
      def toTableDescription(assimilationPath: JoinedAssimilationPath[_])(implicit config: Config, model: Model): Option[String] = assimilationPath.tipDescription
      def toForeignKeyDescription(primaryKeyColumn: Column)(implicit config: Config, model: Model): String = s"Foreign key to ${primaryKeyColumn.description}"
      def toColumnName(tableAssimilationPath: JoinedAssimilationPath[_], columnAssimilationPath: JoinedAssimilationPath[_], repeatIndex: Option[Int] = None, enumeration: Option[Enumeration] = None)(implicit config: Config, model: Model): String = {
        val cleansed = cleanAndUpperCase({
          if (tableAssimilationPath == columnAssimilationPath) columnAssimilationPath.tipName
          else {
            val relative = columnAssimilationPath.relativeTo(tableAssimilationPath)
            val common = relative.assimilationPath.commonAssimilationReferences
            val joined = (if (!relative.startOnAssimilationReference) common.head.dataType.name() else "") + " " + common.flatMap(_.assimilationReference.name).mkString(" ")
            if (joined.trim.isEmpty) columnAssimilationPath.tipName + (if (enumeration.isDefined) " Type Code" else "") else joined
          }
        })
        cleansed + (repeatIndex match {
          case Some(index) => s"_$index"
          case None => ""
        })
      }
      def toColumnDescription(tableAssimilationPath: JoinedAssimilationPath[_], columnAssimilationPath: JoinedAssimilationPath[_])(implicit config: Config, model: Model): Option[String] = columnAssimilationPath.tipDescription
      def toEnumerationName(assimilationPath: JoinedAssimilationPath[AssimilationReference])(implicit config: Config, model: Model): String = assimilationPath.tipAssimilationReference.assimilation.name.getOrElse(assimilationPath.tipName).replaceAll("\\s+", "")
      def toEnumerationDesciption(assimilationPath: JoinedAssimilationPath[AssimilationReference])(implicit config: Config, model: Model): Option[String] = assimilationPath.tipAssimilationReference.assimilation.description orElse assimilationPath.tipDescription
      def toEnumerationValueName(assimilationPath: JoinedAssimilationPath[DataType])(implicit config: Config, model: Model): String = cleanAndUpperCase(assimilationPath.tipDataType.name())
      def toEnumerationValueDescription(assimilationPath: JoinedAssimilationPath[DataType])(implicit config: Config, model: Model): Option[String] = assimilationPath.tipDescription
    }
  }

  def render(implicit model: Model, config: Config = Config()): RelationalModel = {
    RelationalModel(model.rootDataTypes.flatMap(dt => toTableFromDataType(JoinedAssimilationPath(dt)).toSet).toSet)
  }

  def toPresetColumn(currentTable: TableInformation, assimilationPath: JoinedAssimilationPath[AssimilationReference])(implicit config: Config, model: Model): Column = {
    def column(columnType: ColumnType, suffix: String = "") = Column(
      name = config.namingPolicy.toColumnName(currentTable.assimilationPath, assimilationPath),
      description = config.namingPolicy.toColumnDescription(currentTable.assimilationPath, assimilationPath),
      `type` = columnType,
      isPrimaryKey = false,
      assimilationPath = assimilationPath)
    import Preset.Assimilation._
    assimilationPath.tipAssimilation match {
      case Identifier => column(ColumnType.VariableCharacter(config.identifierLength))
      case DateTime => column(ColumnType.DateTime)
      case Date => column(ColumnType.Date)
      case IntegralNumber => column(ColumnType.Integer)
      case DecimalNumber => column(ColumnType.Number(10, 5))
      case Boolean => column(ColumnType.FixedCharacter(1))
      case Code2 => column(ColumnType.FixedCharacter(2))
      case Code3 => column(ColumnType.FixedCharacter(3))
      case TextBlock => column(ColumnType.VariableCharacter(500))
      case ShortName => column(ColumnType.VariableCharacter(50))
      case LongName => column(ColumnType.VariableCharacter(100))
      case x => throw new IllegalArgumentException(s"${x.filePath} is not accounted for...")
    }
  }

  case class TableInformation(assimilationPath: JoinedAssimilationPath[_], explicitPrimaryKeys: Seq[Column] = Seq()) {
    def name(implicit config: Config, model: Model) = config.namingPolicy.toTableName(assimilationPath)
    def primaryKeys(implicit config: Config, model: Model) = if (explicitPrimaryKeys.isEmpty) Seq(Column(name + "_SK", Some(s"Surrogate Key (system generated) for the ${name} table."), ColumnType.Number(15, 0), assimilationPath, true)) else explicitPrimaryKeys
    def foreignKeyReferences(implicit config: Config, model: Model) = primaryKeys.map(pk => ColumnReference(name, pk.name))
    def foreignKeys(implicit config: Config, model: Model) = primaryKeys.map(pk => Column(
      name = pk.name,
      description = Some(config.namingPolicy.toForeignKeyDescription(pk)),
      `type` = pk.`type`,
      pk.assimilationPath, // Hmmm... Not sure what should be here
      foreignKeyReference = Some(pk.toForeignKeyReference(name))))
    def primaryForeignKeys(implicit config: Config, model: Model) = foreignKeys.map(_.copy(isPrimaryKey = true))
  }
  
  def isNonGeneratedColumn(parent: TableInformation, c: Column)(implicit config: Config, model: Model) = !c.isPrimaryKey && c.foreignKeyReference.map(_.tableName != parent.name).getOrElse(true)
  def consumeColumns(parentAssimilationPath: JoinedAssimilationPath[_], cs: Seq[Column])(implicit config: Config, model: Model) = cs.map(c => c.copy(name = config.namingPolicy.toColumnName(parentAssimilationPath, c.assimilationPath, c.repeatIndex)))

  def columnsAndChildTables(currentTableInformation: TableInformation, assimilationPath: JoinedAssimilationPath[AssimilationReference])(implicit config: Config, model: Model): (Seq[Column], Set[TableSet]) = {
    val tableSet = toTableFromAssimilationReference(assimilationPath, currentTableInformation)
    val (columns, childTables) = tableSet.migrateHeadColumnsTo(currentTableInformation)
    val comsumedColumns = consumeColumns(currentTableInformation.assimilationPath, columns.filter(c => isNonGeneratedColumn(currentTableInformation, c)))
    assimilationPath.tipAssimilationReference.assimilationReference.maximumOccurences match {
      case Some(maxOccurs) =>
        if (maxOccurs > 1 && maxOccurs <= config.maximumOccurencesTableLimit && tableSet.childTableSets.isEmpty)
          (for {
            i <- (1 to (maxOccurs))
            c <- comsumedColumns
          } yield c.copy(name = config.namingPolicy.toColumnName(currentTableInformation.assimilationPath, c.assimilationPath, Some(i)), repeatIndex = Some(i)), Set())
        else if (maxOccurs == 1) (comsumedColumns, childTables)
        else (Seq(), Set(tableSet))
      case None =>
        (Seq(), Set(tableSet))
    }
  }

  def mergeJoinableChildTableSets(childTableSets: Set[TableSet])(implicit config: Config, model: Model): Set[TableSet] = {
    def recurse(mergedCts: Set[TableSet], remainingCts: Set[TableSet]): Set[TableSet] = {
      if (remainingCts.isEmpty) mergedCts
      else {
        val (headSet, tailSet) = remainingCts.splitAt(1)
        val (mergeable, notMergeable) = tailSet.partition(_.isMergeableWith(headSet.head))
        val newMergedCts = mergedCts + mergeable.foldLeft(headSet.head)((merged, next) => merged.mergeWith(next))
        recurse(newMergedCts, notMergeable)
      }
    }
    recurse(Set(), childTableSets)
  }

  def toTableFromAssimilationReference(assimilationPath: JoinedAssimilationPath[AssimilationReference], parent: TableInformation)(implicit config: Config, model: Model): TableSet = {
    if (assimilationPath.tipAssimilation.isPreset) {
      val ti = TableInformation(assimilationPath)
      TableSet(Table(
        name = ti.name,
        description = config.namingPolicy.toTableDescription(ti.assimilationPath),
        columns = ti.primaryKeys ++ parent.foreignKeys :+ toPresetColumn(ti, assimilationPath),
        assimilationPath = assimilationPath), Set())
    } else if (assimilationPath.tipDataTypes.size == 1) toTableFromDataType(assimilationPath + assimilationPath.singleTipDataType, Some(parent))
    else if (assimilationPath.tipDataTypes.size > 1) {
      val subTypeParent = TableInformation(assimilationPath)
      val (consumedColumns, childTables) = assimilationPath.tipDataTypes.map(dt => toTableFromDataType(assimilationPath + dt, Some(subTypeParent), true)).foldLeft((Seq.empty[Column], Set.empty[TableSet])) {
        case ((fcs, fts), ts) =>
          if (!config.maximumSubTypeColumnsBeforeSplittingOut.isDefined || ts.table.columns.filter(c => isNonGeneratedColumn(subTypeParent, c)).size <= config.maximumSubTypeColumnsBeforeSplittingOut.get) {
            val (columns, migratedChildTables) = ts.migrateHeadColumnsTo(subTypeParent)
            (fcs ++ consumeColumns(assimilationPath, columns.filter(c => isNonGeneratedColumn(subTypeParent, c))), fts ++ migratedChildTables)
          } else (fcs, fts + ts)
      }
      val e = toEnumeration(assimilationPath)
      TableSet(Table(
        name = subTypeParent.name,
        description = config.namingPolicy.toTableDescription(subTypeParent.assimilationPath),
        columns = subTypeParent.primaryKeys ++ parent.foreignKeys ++
          {
            Column(
              name = config.namingPolicy.toColumnName(subTypeParent.assimilationPath, assimilationPath, None, Some(e)),
              description = assimilationPath.tipAssimilationReference.assimilationReference.description,
              `type` = ColumnType.VariableCharacter(e.maximumValueSize),
              enumeration = Some(e),
              assimilationPath = assimilationPath) +: consumedColumns
          },
        assimilationPath = assimilationPath), mergeJoinableChildTableSets(childTables))
    } else throw new IllegalStateException(s"It is illegal for assimililation '${assimilationPath.tipAssimilationReference.assimilationReference.filePath}' to have no data type references.")
  }

  def toEnumeration(assimilationPath: JoinedAssimilationPath[AssimilationReference])(implicit config: Config, model: Model) =
    Enumeration(config.namingPolicy.toEnumerationName(assimilationPath),
      assimilationPath.tipDataTypes.map {
        dt =>
          val valueAssimilationPath = assimilationPath + dt
          EnumerationValue(config.namingPolicy.toEnumerationValueName(valueAssimilationPath), config.namingPolicy.toEnumerationValueDescription(valueAssimilationPath), valueAssimilationPath)
      }, assimilationPath)

  def toTableFromDataType(assimilationPath: JoinedAssimilationPath[DataType], parent: Option[TableInformation] = None, definingRelationship: Boolean = false)(implicit config: Config, model: Model): TableSet = {
    val ti = TableInformation(assimilationPath, if (definingRelationship) parent.get.primaryForeignKeys else Seq())
    val (columns, childTables) = assimilationPath.tipDataType.assimilationReferences.map(ar => columnsAndChildTables(ti, assimilationPath + ar)).foldLeft((Seq.empty[Column], Set.empty[TableSet])) {
      case ((fcs, fts), (cs, ts)) => (fcs ++ cs, fts ++ ts)
    }
    TableSet(Table(
      ti.name,
      config.namingPolicy.toTableDescription(assimilationPath),
      ti.primaryKeys ++ { if (definingRelationship || !parent.isDefined) Seq() else parent.get.foreignKeys } ++ columns,
      assimilationPath), mergeJoinableChildTableSets(childTables))
  }

}