package com.typeassimilation.model

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
    def isMergeableWith(other: TableSet) = table.foreignKeys.map(fk => (fk.name, fk.`type`, fk.foreignKeyReference)) == other.table.foreignKeys.map(fk => (fk.name, fk.`type`, fk.foreignKeyReference)) &&
      table.assimilationPath.tip == other.table.assimilationPath.tip &&
      table.assimilationPath.parents.size == 1 && other.table.assimilationPath.parents.size == 1 &&
      table.assimilationPath.parents.head.tip == other.table.assimilationPath.parents.head.tip
    def mergeWith(other: TableSet, parentTableInformation: Option[TableInformation])(implicit config: Config, model: Model): TableSet = {
      def recurse(pti: Option[TableInformation], ts1: TableSet, ts2: TableSet): TableSet = {
        ts1.table.validate
        ts2.table.validate
        val mergedTableAssimilationPath = ts1.table.assimilationPath + ts2.table.assimilationPath
        val mergedPrimaryKeys = ts1.table.primaryKeys.map {
          pk1 =>
            val pk2 = ts2.table.column(pk1.name).get
            val mergedPkAssimilationPath = pk1.assimilationPath + pk2.assimilationPath
            pk1.copy(assimilationPath = mergedPkAssimilationPath)
        }
        val mergedTableInformation = TableInformation(parentTableInformation, mergedTableAssimilationPath, mergedPrimaryKeys)
        def mergeColumns(c1: Column, c2: Column) = {
          val mergedColumnAssimilationPath = c1.assimilationPath + c2.assimilationPath
          c1.copy(
            name = config.namingPolicy.toColumnName(mergedTableInformation, mergedColumnAssimilationPath, c1.repeatIndex, c1.enumeration),
            description = config.namingPolicy.toColumnDescription(mergedTableInformation.assimilationPath, mergedColumnAssimilationPath),
            isGenerated = c1.isGenerated,
            isNullable = c1.isNullable,
            assimilationPath = mergedColumnAssimilationPath,
            enumeration = c1.enumeration.map(e1 => toEnumeration(e1.assimilationPath + c2.enumeration.get.assimilationPath)))
        }
        TableSet(
          table = {
            val (ts1ParentForeignKeyColumns, ts1OtherColumns) = ts1.table.columns.filterNot(c => c.isPrimaryKey && !c.foreignKeyReference.isDefined).partition(c => c.foreignKeyReference.isDefined && c.foreignKeyReference.map(_.tableName) == pti.map(_.name))
            if (pti.isDefined && ts1ParentForeignKeyColumns.isEmpty) throw new IllegalArgumentException(s"The parent table information appears to be inconsistent with the foreign keys present.")
            val otherForeignKeys = ts1OtherColumns.filter(_.foreignKeyReference.isDefined)
            Table(
              name = config.namingPolicy.toTableName(pti, mergedTableInformation.assimilationPath),
              description = config.namingPolicy.toTableDescription(mergedTableInformation.assimilationPath),
              columns =
                mergedTableInformation.primaryKeys ++
                  pti.toSeq.flatMap(_.foreignKeys(mergedTableInformation.assimilationPath, mergedTableInformation.primaryKeys)) ++
                  ts1.table.nonKeyColumns.map(c => mergeColumns(c, ts2.table.column(c.name).get)), 
              assimilationPath = mergedTableInformation.assimilationPath)
          },
          childTableSets = {
            val cts2Map = ts2.childTableSets.map(cts2 => cts2.table.name -> cts2).toMap
            ts1.childTableSets.map(cts1 => recurse(Some(mergedTableInformation), cts1, cts2Map(cts1.table.name)))
          })
      }
      recurse(parentTableInformation, this, other)
    }
    def migrateHeadColumnsTo(newParent: TableInformation)(implicit config: Config, model: Model): (Seq[Column], Set[TableSet]) = (
      table.columns.filter(c => !c.isPrimaryKey && !c.foreignKeyReference.isDefined),
      childTableSets.map(ts => ts.copy(table = ts.table.copy(
        name = config.namingPolicy.toTableName(Some(newParent), ts.table.assimilationPath),
        columns = {
          val previousParentKeysHead = ts.table.columns.filter(c => c.foreignKeyReference.isDefined && c.foreignKeyReference.get.tableName == table.name).head
          val previousParentAssimilationPath = previousParentKeysHead.assimilationPath
          val columnsWithRemovedPreviousParent = ts.table.columns.filterNot(c => c.foreignKeyReference.isDefined && c.foreignKeyReference.get.tableName == table.name)
          val keys = columnsWithRemovedPreviousParent.filter(_.isPrimaryKey) match {
            case pks if pks.isEmpty => newParent.primaryForeignKeys(previousParentAssimilationPath)
            case pks => if (previousParentKeysHead.isPrimaryKey) newParent.primaryForeignKeys(previousParentAssimilationPath) ++ pks else pks ++ newParent.foreignKeys(previousParentAssimilationPath)
          }
          keys ++ columnsWithRemovedPreviousParent.filter(!_.isPrimaryKey)
        }))))
  }
  case class Table(name: String, description: Option[String], columns: Seq[Column], assimilationPath: JoinedAssimilationPath[_]) {
    private val nameToColumnMap = columns.map(c => c.name -> c).toMap
    def column(name: String) = nameToColumnMap.get(name)
    def primaryKeys = columns.filter(_.isPrimaryKey)
    def primaryForeignKeys = primaryKeys.filter(_.foreignKeyReference.isDefined)
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
  case class Column(name: String, description: Option[String], `type`: ColumnType, isGenerated: Boolean, isNullable: Boolean, assimilationPath: JoinedAssimilationPath[_], isPrimaryKey: Boolean = false, enumeration: Option[Enumeration] = None, foreignKeyReference: Option[ColumnReference] = None, repeatIndex: Option[Int] = None) {
    override def toString = s"$name " + `type`.toString + (if (enumeration.isDefined) s" (ENUM: ${enumeration.get.name})" else "") + {
      if (isPrimaryKey && foreignKeyReference.isEmpty) "(PK)"
      else if (!isPrimaryKey && foreignKeyReference.isDefined) "(FK -> " + foreignKeyReference.get + ")"
      else if (isPrimaryKey && foreignKeyReference.isDefined) "(PFK -> " + foreignKeyReference.get + ")"
      else ""
    } + (if(isNullable) " NULL" else " NOT NULL") + (if(isGenerated) " <GENERATED>" else "") + (if (description.isDefined) s": ${description.get}" else "") + s" [$assimilationPath]"
    def toForeignKeyReference(tableName: String) = ColumnReference(tableName, name)
  }
  case class ColumnReference(tableName: String, columnName: String) {
    override def toString = s"$tableName.$columnName"
  }
  case class Enumeration(name: String, values: Seq[EnumerationValue], assimilationPath: JoinedAssimilationPath[Assimilation]) {
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
    maximumSubTypeColumnsBeforeSplittingOut: Option[Int] = Some(4),
    namingPolicy: NamingPolicy = NamingPolicy.Default,
    primaryKeyCreationPolicy: PrimaryKeyCreationPolicy = PrimaryKeyCreationPolicy.SurrogateKeyGeneration)

  trait NamingPolicy {
    def toTableName(parentTableInformation: Option[TableInformation], assimilationPath: JoinedAssimilationPath[_])(implicit config: Config, model: Model): String
    def toTableDescription(assimilationPath: JoinedAssimilationPath[_])(implicit config: Config, model: Model): Option[String]
    def toForeignKeyDescription(primaryKeyColumn: Column)(implicit config: Config, model: Model): String
    def toColumnName(tableInformation: TableInformation, columnAssimilationPath: JoinedAssimilationPath[_], repeatIndex: Option[Int] = None, enumeration: Option[Enumeration] = None)(implicit config: Config, model: Model): String
    def toColumnDescription(tableAssimilationPath: JoinedAssimilationPath[_], columnAssimilationPath: JoinedAssimilationPath[_])(implicit config: Config, model: Model): Option[String]
    def toEnumerationName(assimilationPath: JoinedAssimilationPath[Assimilation])(implicit config: Config, model: Model): String
    def toEnumerationDesciption(assimilationPath: JoinedAssimilationPath[Assimilation])(implicit config: Config, model: Model): Option[String]
    def toEnumerationValueName(assimilationPath: JoinedAssimilationPath[DataType])(implicit config: Config, model: Model): String
    def toEnumerationValueDescription(assimilationPath: JoinedAssimilationPath[DataType])(implicit config: Config, model: Model): Option[String]
  }

  object NamingPolicy {
    object Default extends NamingPolicy {
      private def cleanAndUpperCase(s: String) = mutable.LinkedHashSet(s.trim.replaceAll("\\s+", "_").toUpperCase.split('_'): _*).mkString("_")
      private def toName(parentAssimilationPath: Option[JoinedAssimilationPath[_]], assimilationPath: JoinedAssimilationPath[_])(implicit model: Model) = {
        def nameElement(a: AbsoluteAssimilation, nextDataTypeOption: Option[DataType]) = (a.assimilation.name orElse a.assimilation.name) orElse nextDataTypeOption.map(_.name)
        parentAssimilationPath match {
          case None => cleanAndUpperCase(assimilationPath.tipName)
          case Some(pap) =>
            val commonReferences = assimilationPath.relativeTo(pap).assimilationPath.commonAssimilations
            val commonNamesWithoutFinal = {
              if (commonReferences.size <= 1) Seq.empty[String]
              else (for {
                (former, latter) <- commonReferences.sliding(2).map(pair => (pair(0), pair(1)))
              } yield nameElement(former, Some(latter.dataType))).toSeq.flatten
            }
            val tipNameOption = if (!commonReferences.isEmpty) nameElement(commonReferences.last, assimilationPath.tipDataTypeOption)
            else assimilationPath.tipDataTypeOption.map(_.name)
            cleanAndUpperCase((commonNamesWithoutFinal ++ tipNameOption).mkString(" "))
        }
      }
      def toTableName(parentTableInformation: Option[TableInformation], assimilationPath: JoinedAssimilationPath[_])(implicit config: Config, model: Model): String = {
        val proposedName = toName(parentTableInformation.map(_.assimilationPath), assimilationPath)
        if (assimilationPath.isReferenceToOrientatingDataType) {
          if (proposedName == toTableName(None, JoinedAssimilationPath(assimilationPath.tipDataType))) {
            parentTableInformation.get.name + "_" + proposedName
          } else proposedName
        } else proposedName
      }
      def toTableDescription(assimilationPath: JoinedAssimilationPath[_])(implicit config: Config, model: Model): Option[String] = assimilationPath.tipDescription
      def toForeignKeyDescription(primaryKeyColumn: Column)(implicit config: Config, model: Model): String = s"Foreign key to ${primaryKeyColumn.description.getOrElse(primaryKeyColumn.foreignKeyReference.get)}"
      def toColumnName(tableInformation: TableInformation, columnAssimilationPath: JoinedAssimilationPath[_], repeatIndex: Option[Int] = None, enumeration: Option[Enumeration] = None)(implicit config: Config, model: Model): String = {
        val cleansed = cleanAndUpperCase({
          if (tableInformation.assimilationPath == columnAssimilationPath) toTableName(tableInformation.parentTableInformation, tableInformation.assimilationPath)
          else toName(Some(tableInformation.assimilationPath), columnAssimilationPath) match {
            case "" => toTableName(tableInformation.parentTableInformation, tableInformation.assimilationPath)
            case x => x
          }
        } + (if (enumeration.isDefined) " Type Code" else ""))
        cleansed + (repeatIndex match {
          case Some(index) => s"_$index"
          case None => ""
        })
      }
      def toColumnDescription(tableAssimilationPath: JoinedAssimilationPath[_], columnAssimilationPath: JoinedAssimilationPath[_])(implicit config: Config, model: Model): Option[String] = columnAssimilationPath.tipDescription
      def toEnumerationName(assimilationPath: JoinedAssimilationPath[Assimilation])(implicit config: Config, model: Model): String = assimilationPath.tipAssimilation.assimilation.name.getOrElse(assimilationPath.tipName).replaceAll("\\s+", "")
      def toEnumerationDesciption(assimilationPath: JoinedAssimilationPath[Assimilation])(implicit config: Config, model: Model): Option[String] = assimilationPath.tipAssimilation.assimilation.description orElse assimilationPath.tipDescription
      def toEnumerationValueName(assimilationPath: JoinedAssimilationPath[DataType])(implicit config: Config, model: Model): String = cleanAndUpperCase(assimilationPath.tipDataType.name)
      def toEnumerationValueDescription(assimilationPath: JoinedAssimilationPath[DataType])(implicit config: Config, model: Model): Option[String] = assimilationPath.tipDescription
    }
  }

  trait PrimaryKeyCreationPolicy {
    def primaryKeys(parentTableInformation: Option[TableInformation], tableName: String, tableAssimilationPath: JoinedAssimilationPath[_])(implicit config: Config, model: Model): Seq[Column]
    def nonPrimaryKeyAssimilations(dataType: DataType): Seq[Assimilation]
    def finalTableSetsFilter(tableSets: Set[TableSet]): Set[TableSet]
  }

  object PrimaryKeyCreationPolicy {
    object SurrogateKeyGeneration extends PrimaryKeyCreationPolicy {
      def primaryKeys(parentTableInformation: Option[TableInformation], tableName: String, tableAssimilationPath: JoinedAssimilationPath[_])(implicit config: Config, model: Model): Seq[Column] = {
        Seq(Column(tableName + "_SK", Some(s"Surrogate Key (system isGenerated) for the ${tableName} table."), ColumnType.Number(15, 0), true, false, tableAssimilationPath, true))
      }
      def nonPrimaryKeyAssimilations(dataType: DataType): Seq[Assimilation] = dataType.assimilations
      def finalTableSetsFilter(tableSets: Set[TableSet]): Set[TableSet] = tableSets
    }
    case class NaturalKey(isGeneratedIdentifierSuffix: String = "_ID") extends PrimaryKeyCreationPolicy {
      def primaryKeys(parentTableInformation: Option[TableInformation], tableName: String, tableAssimilationPath: JoinedAssimilationPath[_])(implicit config: Config, model: Model): Seq[Column] = {
        def generatePrimaryKey = parentTableInformation.toSeq.flatMap(_.primaryForeignKeys(tableAssimilationPath)) ++ Seq(Column(tableName + isGeneratedIdentifierSuffix, Some(s"The unique identifier for the ${tableAssimilationPath.tipName}."), ColumnType.Number(15, 0), true, false, tableAssimilationPath, true))
        tableAssimilationPath.tip match {
          case Left(dataType) => if (dataType.identifyingAssimilations.isEmpty) generatePrimaryKey else {
            val identifyingAssimilation = dataType.identifyingAssimilations.head
            if (identifyingAssimilation.minimumOccurences != Some(1) || identifyingAssimilation.maximumOccurences != Some(1)) throw new IllegalStateException(s"The identifying assimilation on '${dataType.filePath}' must have minimum and maximum occurrences of exactly 1 to use the relational renderer.")
            val dummyTableInformation = TableInformation(parentTableInformation, tableAssimilationPath, generatePrimaryKey)
            val (columns, childTableSets) = columnsAndChildTables(dummyTableInformation, tableAssimilationPath + identifyingAssimilation)
            if (!childTableSets.isEmpty) throw new IllegalStateException(s"The identifying assimilation in '${dataType.filePath}' must not result in child tables - you must assimilate types that will resolve to columns alone.")
            else columns.map(_.copy(isPrimaryKey = true))
          }
          case Right(assimilation) => generatePrimaryKey
        }
      }
      def nonPrimaryKeyAssimilations(dataType: DataType): Seq[Assimilation] =
        if (dataType.identifyingAssimilations.size > 1) throw new IllegalStateException(s"The Natural Key primary key policy only supports data types with a single identifying assimilation, ${dataType.filePath} does not.")
        else dataType.assimilations.filterNot(_.isIdentifying)
      def finalTableSetsFilter(tableSets: Set[TableSet]): Set[TableSet] = {
        def removePrimaryKeysIfNoChildTables(tableSet: TableSet): TableSet = if (tableSet.childTableSets.isEmpty) {
          if (tableSet.table.primaryKeys.size > tableSet.table.primaryForeignKeys.size) {
            tableSet.copy(table = tableSet.table.copy(columns = tableSet.table.columns.flatMap {
              c => if (c.isPrimaryKey) {
                if (c.foreignKeyReference.isDefined) Some(c.copy(isPrimaryKey = false))
                else None
              }
              else Some(c)
            }))
          } else tableSet
        }
        else tableSet.copy(childTableSets = tableSet.childTableSets.map(removePrimaryKeysIfNoChildTables))
        tableSets.map(removePrimaryKeysIfNoChildTables)
      }
    }
  }

  def render(implicit model: Model, config: Config = Config()): RelationalModel = {
    RelationalModel(config.primaryKeyCreationPolicy.finalTableSetsFilter(
      model.orientatingDataTypes.map(dt => toTableFromDataType(JoinedAssimilationPath(dt)))).flatMap(_.toSet))
  }

  def isNullable(assimilation: Assimilation): Boolean = assimilation.minimumOccurences.getOrElse(0) == 0
  
  def toPrimitiveColumn(currentTable: TableInformation, assimilationPath: JoinedAssimilationPath[Assimilation])(implicit config: Config, model: Model): Column = {
    def column(columnType: ColumnType, suffix: String = "") = Column(
      name = config.namingPolicy.toColumnName(currentTable, assimilationPath),
      description = config.namingPolicy.toColumnDescription(currentTable.assimilationPath, assimilationPath),
      `type` = columnType,
      isPrimaryKey = false,
      isGenerated = false,
      isNullable = isNullable(assimilationPath.tipAssimilation.assimilation),
      assimilationPath = assimilationPath)
    import Preset.DataType._
    assimilationPath.singleTipDataType match {
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

  case class TableInformation(parentTableInformation: Option[TableInformation], assimilationPath: JoinedAssimilationPath[_], explicitPrimaryKeys: Seq[Column] = Seq()) {
    def name(implicit config: Config, model: Model) = config.namingPolicy.toTableName(parentTableInformation, assimilationPath)
    def primaryKeys(implicit config: Config, model: Model) = if (explicitPrimaryKeys.isEmpty) config.primaryKeyCreationPolicy.primaryKeys(parentTableInformation, name, assimilationPath) else explicitPrimaryKeys
    def foreignKeys(referencingAssimilationPath: JoinedAssimilationPath[_], currentPrimaryKeys: Seq[Column] = Seq())(implicit config: Config, model: Model) =
      if (currentPrimaryKeys.flatMap(_.foreignKeyReference).containsSlice(primaryKeys.map(_.toForeignKeyReference(name)))) Seq()
      else primaryKeys.map(pk => Column(
        name = pk.name,
        description = Some(config.namingPolicy.toForeignKeyDescription(pk)),
        `type` = pk.`type`,
        isGenerated = pk.isGenerated,
        isNullable = false,
        assimilationPath = referencingAssimilationPath,
        foreignKeyReference = Some(pk.toForeignKeyReference(name))))
    def primaryForeignKeys(referencingAssimilationPath: JoinedAssimilationPath[_])(implicit config: Config, model: Model) = foreignKeys(referencingAssimilationPath).map(_.copy(isPrimaryKey = true))
  }

  def isNonGeneratedColumn(parent: TableInformation, c: Column)(implicit config: Config, model: Model) = !c.isPrimaryKey && c.foreignKeyReference.map(_.tableName != parent.name).getOrElse(true)
  def consumeColumns(parentTableInformation: TableInformation, cs: Seq[Column])(implicit config: Config, model: Model) = cs.map(c => c.copy(
      name = config.namingPolicy.toColumnName(parentTableInformation, c.assimilationPath, c.repeatIndex, c.enumeration),
      isNullable = isNullable(c.assimilationPath.relativeTo(parentTableInformation.assimilationPath).assimilationPath.commonAssimilations.head.assimilation)    
  ))

  def columnsAndChildTables(currentTableInformation: TableInformation, assimilationPath: JoinedAssimilationPath[Assimilation])(implicit config: Config, model: Model): (Seq[Column], Set[TableSet]) = {
    val tableSet = toTableFromAssimilation(assimilationPath, currentTableInformation)
    val (columns, childTables) = tableSet.migrateHeadColumnsTo(currentTableInformation)
    val comsumedColumns = consumeColumns(currentTableInformation, columns.filter(c => isNonGeneratedColumn(currentTableInformation, c)))
    assimilationPath.tipAssimilation.assimilation.maximumOccurences match {
      case Some(maxOccurs) =>
        if (maxOccurs > 1 && maxOccurs <= config.maximumOccurencesTableLimit && tableSet.childTableSets.isEmpty)
          (for {
            i <- (1 to (maxOccurs))
            c <- comsumedColumns
          } yield c.copy(name = config.namingPolicy.toColumnName(currentTableInformation, c.assimilationPath, Some(i), c.enumeration), repeatIndex = Some(i)), Set())
        else if (maxOccurs == 1) (comsumedColumns, childTables)
        else (Seq(), Set(tableSet))
      case None =>
        (Seq(), Set(tableSet))
    }
  }

  def mergeJoinableChildTableSets(parentTableInformation: Option[TableInformation], childTableSets: Set[TableSet])(implicit config: Config, model: Model): Set[TableSet] = {
    def recurse(mergedCts: Set[TableSet], remainingCts: Set[TableSet]): Set[TableSet] = {
      if (remainingCts.isEmpty) mergedCts
      else {
        val (headSet, tailSet) = remainingCts.splitAt(1)
        val (mergeable, notMergeable) = tailSet.partition(_.isMergeableWith(headSet.head))
        val newMergedCts = mergedCts + mergeable.foldLeft(headSet.head)((merged, next) => merged.mergeWith(next, parentTableInformation))
        recurse(newMergedCts, notMergeable)
      }
    }
    recurse(Set(), childTableSets)
  }

  def toTableFromAssimilation(assimilationPath: JoinedAssimilationPath[Assimilation], parent: TableInformation)(implicit config: Config, model: Model): TableSet = {
    if (assimilationPath.tipAssimilation.dataTypeFilePaths.size == 1 && assimilationPath.singleTipDataType.primitive) {
      val ti = TableInformation(Some(parent), assimilationPath)
      TableSet(Table(
        name = ti.name,
        description = config.namingPolicy.toTableDescription(ti.assimilationPath),
        columns = ti.primaryKeys ++ parent.foreignKeys(assimilationPath, ti.primaryKeys) :+ toPrimitiveColumn(ti, assimilationPath),
        assimilationPath = assimilationPath), Set())
    } else if (assimilationPath.tipDataTypes.size == 1) toTableFromDataTypeOrReference(assimilationPath + assimilationPath.singleTipDataType, Some(parent))
    else if (assimilationPath.tipDataTypes.size > 1) {
      val subTypeParent = TableInformation(Some(parent), assimilationPath)
      val (consumedColumns, childTables) = assimilationPath.tipDataTypes.map(dt => toTableFromDataTypeOrReference(assimilationPath + dt, Some(subTypeParent), true)).foldLeft((Seq.empty[Column], Set.empty[TableSet])) {
        case ((fcs, fts), ts) =>
          if (!config.maximumSubTypeColumnsBeforeSplittingOut.isDefined || ts.table.columns.filter(c => isNonGeneratedColumn(subTypeParent, c)).size <= config.maximumSubTypeColumnsBeforeSplittingOut.get) {
            val (columns, migratedChildTables) = ts.migrateHeadColumnsTo(subTypeParent)
            (fcs ++ consumeColumns(subTypeParent, columns.filter(c => isNonGeneratedColumn(subTypeParent, c))), fts ++ migratedChildTables)
          } else (fcs, fts + ts)
      }
      val e = toEnumeration(assimilationPath)
      TableSet(Table(
        name = subTypeParent.name,
        description = config.namingPolicy.toTableDescription(subTypeParent.assimilationPath),
        columns = subTypeParent.primaryKeys ++ parent.foreignKeys(subTypeParent.assimilationPath, subTypeParent.primaryKeys) ++
          {
            Column(
              name = config.namingPolicy.toColumnName(subTypeParent, assimilationPath, None, Some(e)),
              description = assimilationPath.tipAssimilation.assimilation.description,
              `type` = ColumnType.VariableCharacter(e.maximumValueSize),
              enumeration = Some(e),
              isGenerated = false,
              isNullable = isNullable(assimilationPath.tipAssimilation.assimilation),
              assimilationPath = assimilationPath) +: consumedColumns
          },
        assimilationPath = assimilationPath), mergeJoinableChildTableSets(Some(subTypeParent), childTables))
    } else throw new IllegalStateException(s"It is illegal for assimililation '${assimilationPath.tipAssimilation}' to have no data type references.")
  }

  def toEnumeration(assimilationPath: JoinedAssimilationPath[Assimilation])(implicit config: Config, model: Model) =
    Enumeration(config.namingPolicy.toEnumerationName(assimilationPath),
      assimilationPath.tipDataTypes.map {
        dt =>
          val valueAssimilationPath = assimilationPath + dt
          EnumerationValue(config.namingPolicy.toEnumerationValueName(valueAssimilationPath), config.namingPolicy.toEnumerationValueDescription(valueAssimilationPath), valueAssimilationPath)
      }, assimilationPath)

  def toTableFromDataTypeOrReference(assimilationPath: JoinedAssimilationPath[DataType], parent: Option[TableInformation] = None, definingRelationship: Boolean = false)(implicit config: Config, model: Model): TableSet = {
    if (assimilationPath.tipDataType.isOrientating) {
      val ti = TableInformation(parent, assimilationPath, parent.get.primaryForeignKeys(assimilationPath))
      val referencedTable = TableInformation(None, JoinedAssimilationPath(assimilationPath.tipDataType))
      TableSet(Table(
        ti.name,
        config.namingPolicy.toTableDescription(assimilationPath),
        parent.get.primaryForeignKeys(assimilationPath) ++ referencedTable.primaryForeignKeys(assimilationPath),
        assimilationPath), Set.empty[TableSet])
    } else
      toTableFromDataType(assimilationPath, parent, definingRelationship)
  }

  def toTableFromDataType(assimilationPath: JoinedAssimilationPath[DataType], parent: Option[TableInformation] = None, definingRelationship: Boolean = false)(implicit config: Config, model: Model): TableSet = {
    val ti = TableInformation(parent, assimilationPath, if (definingRelationship) parent.get.primaryForeignKeys(assimilationPath) else Seq())
    val (columns, childTables) = config.primaryKeyCreationPolicy.nonPrimaryKeyAssimilations(assimilationPath.tipDataType).map(a => columnsAndChildTables(ti, assimilationPath + a)).foldLeft((Seq.empty[Column], Set.empty[TableSet])) {
      case ((fcs, fts), (cs, ts)) => (fcs ++ cs, fts ++ ts)
    }
    TableSet(Table(
      ti.name,
      config.namingPolicy.toTableDescription(assimilationPath),
      ti.primaryKeys ++ { if (!parent.isDefined) Seq() else parent.get.foreignKeys(assimilationPath, ti.primaryKeys) } ++ columns,
      assimilationPath), mergeJoinableChildTableSets(Some(ti), childTables))
  }

}