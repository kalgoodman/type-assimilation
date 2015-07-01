package com.typeassimilation.model

import Model.Implicits._

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
  case class TableSet(head: Table, tail: Set[Table]) {
    def toSet = tail + head
    def migrateHeadColumnsTo(newParent: TableInformation): (Seq[Column], Set[Table]) = (
      head.columns.filter(c => !c.isPrimaryKey && !c.foreignKeyReference.isDefined),
      tail.map(t => t.copy(columns = {
        val columnsWithRemovedPreviousParent = t.columns.filterNot(c => c.foreignKeyReference.isDefined && c.foreignKeyReference.get.tableName == head.name)
        val keys = columnsWithRemovedPreviousParent.filter(_.isPrimaryKey) match {
          case pks if pks.isEmpty => newParent.primaryForeignKeys
          case pks => pks ++ newParent.foreignKeys
        }
        keys ++ columnsWithRemovedPreviousParent.filter(!_.isPrimaryKey)
      })))
  }
  case class Table(name: String, description: Option[String], columns: Seq[Column], assimilationPath: AssimilationPath[_]) {
    private val nameToColumnMap = columns.map(c => c.name -> c).toMap
    def column(name: String) = nameToColumnMap.get(name)
    def primaryKeys = columns.filter(_.isPrimaryKey)
    def foreignKeys = columns.filter(_.foreignKeyReference.isDefined)
    override def toString = name + (if (description.isDefined) s": ${description.get}" else "") + s" [$assimilationPath] {\n" + columns.map(c => s"\t${c.toString}").mkString("\n") + "\n}\n"
  }
  case class Column(name: String, description: Option[String], `type`: ColumnType, assimilationPath: AssimilationPath[_], isPrimaryKey: Boolean = false, enumeration: Option[Enumeration] = None, foreignKeyReference: Option[ColumnReference] = None) {
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
  case class Enumeration(name: String, values: Seq[EnumerationValue]) {
    def maximumValueSize = values.map(_.name.length).foldLeft(0)((size, v) => if (v > size) v else size)
    override def toString = s"$name {\n" + values.map(v => s"\t${v.toString}").mkString("\n") + "\n}\n"
  }
  case class EnumerationValue(name: String, description: Option[String]) {
    override def toString = name + (if (description.isDefined) s" - ${description.get}" else "")
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
    maximumSubTypeColumnsBeforeSplittingOut: Option[Int] = Some(4))

  def render(implicit model: Model, config: Config = Config()): RelationalModel = {
    RelationalModel(model.rootDataTypes.flatMap(dt => toTableFromDataType(AssimilationPath(dt)).toSet).toSet)
  }

  def toTableName(dataTypeName: String) = dataTypeName.trim.replaceAll("\\s+", "_").toUpperCase
  def toTableDescription(dataTypeDescription: String) = dataTypeDescription
  def toForeignKeyDescription(primaryKeyDescription: String) = s"Foreign key to ${primaryKeyDescription}"
  def toColumnName(assimilationName: String) = toTableName(assimilationName)
  def toEnumerationName(enumerationName: String) = toTableName(enumerationName)
  def toEnumerationValueName(enumerationValueName: String) = toEnumerationName(enumerationValueName)

  def toPresetColumns(currentTableName: String, assimilationPath: AssimilationPath[AssimilationReference])(implicit config: Config, model: Model): Seq[Column] = {
    def column(columnType: ColumnType, suffix: String = "") = Column(
      name = assimilationPath.tipAssimilationReference.assimilationReference.name.map(toColumnName).getOrElse(assimilationPath.tipAssimilation.singleDataType.name()) + suffix,
      description = assimilationPath.tipDescription,
      `type` = columnType,
      isPrimaryKey = false,
      assimilationPath = assimilationPath)
    import Preset.Assimilation._
    assimilationPath.tipAssimilation match {
      case Identifier => Seq(column(ColumnType.VariableCharacter(config.identifierLength)))
      case DateTime => Seq(column(ColumnType.DateTime))
      case Date => Seq(column(ColumnType.Date))
      case Money => Seq(column(ColumnType.Number(5, 3), config.moneyAmountSuffix), column(ColumnType.FixedCharacter(3), config.moneyCurrencyCodeSuffix))
      case IntegralNumber => Seq(column(ColumnType.Integer))
      case DecimalNumber => Seq(column(ColumnType.Number(10, 5)))
      case Boolean => Seq(column(ColumnType.FixedCharacter(1)))
      case TextBlock => Seq(column(ColumnType.VariableCharacter(500)))
      case ShortName => Seq(column(ColumnType.VariableCharacter(50)))
      case LongName => Seq(column(ColumnType.VariableCharacter(100)))
    }
  }

  case class TableInformation(name: String, primaryKeys: Seq[Column]) {
    def foreignKeyReferences = primaryKeys.map(pk => ColumnReference(name, pk.name))
    def foreignKeys = primaryKeys.map(pk => Column(
      name = pk.name,
      description = pk.description.map(toForeignKeyDescription),
      `type` = pk.`type`,
      pk.assimilationPath, // Hmmm... Not sure what should be here
      foreignKeyReference = Some(pk.toForeignKeyReference(name))))
    def primaryForeignKeys = foreignKeys.map(_.copy(isPrimaryKey = true))
  }

  def isNonGeneratedColumn(parent: TableInformation, c: Column) = !c.isPrimaryKey && c.foreignKeyReference.map(_.tableName != parent.name).getOrElse(true)
  def consumeColumns(assimilationPath: AssimilationPath[AssimilationReference], cs: Seq[Column]) = cs.map(c => c.copy(name = {
    val newParentName = toColumnName(assimilationPath.tipName)
    if (c.name.contains(newParentName)) c.name else toColumnName(s"$newParentName ${c.name}")
  }))

  def columnsAndChildTables(currentTableInformation: TableInformation, assimilationPath: AssimilationPath[AssimilationReference])(implicit config: Config, model: Model): (Seq[Column], Set[Table]) = {
    val tableSet = toTableFromAssimilationReference(assimilationPath, currentTableInformation)
    val (columns, childTables) = tableSet.migrateHeadColumnsTo(currentTableInformation)
    val comsumedColumns = consumeColumns(assimilationPath, columns.filter(c => isNonGeneratedColumn(currentTableInformation, c)))
    assimilationPath.tipAssimilationReference.assimilationReference.maximumOccurences match {
      case Some(maxOccurs) =>
        if (maxOccurs > 1 && maxOccurs <= config.maximumOccurencesTableLimit && tableSet.tail.isEmpty)
          (for {
            i <- (1 to (maxOccurs))
            c <- comsumedColumns
          } yield c.copy(name = s"${c.name}_$i"), Set())
        else if (maxOccurs == 1) (comsumedColumns, childTables)
        else (Seq(), tableSet.toSet)
      case None =>
        (Seq(), tableSet.toSet)
    }
  }

  def toTableFromAssimilationReference(assimilationPath: AssimilationPath[AssimilationReference], parent: TableInformation)(implicit config: Config, model: Model): TableSet = {
    if (assimilationPath.tipAssimilation.isPreset) {
      val ti = createTableInformation(assimilationPath)
      TableSet(Table(
        name = ti.name,
        description = assimilationPath.tipDescription.map(toTableDescription),
        columns = ti.primaryKeys ++ parent.foreignKeys ++ toPresetColumns(ti.name, assimilationPath),
        assimilationPath = assimilationPath), Set())
    } else if (assimilationPath.tipDataTypes.size == 1) toTableFromDataType(assimilationPath + assimilationPath.singleTipDataType, Some(parent))
    else if (assimilationPath.tipDataTypes.size > 1) {
      val subTypeParent = createTableInformation(assimilationPath)
      val (consumedColumns, childTables) = assimilationPath.tipDataTypes.map(dt => toTableFromDataType(assimilationPath + dt, Some(subTypeParent), true)).foldLeft((Seq.empty[Column], Set.empty[Table])) {
        case ((fcs, fts), ts) =>
          if (!config.maximumSubTypeColumnsBeforeSplittingOut.isDefined || ts.head.columns.filter(c => isNonGeneratedColumn(subTypeParent, c)).size <= config.maximumSubTypeColumnsBeforeSplittingOut.get) {
            val (columns, migratedChildTables) = ts.migrateHeadColumnsTo(subTypeParent)
            (fcs ++ consumeColumns(assimilationPath, columns.filter(c => isNonGeneratedColumn(subTypeParent, c))), fts ++ migratedChildTables)
          }
          else (fcs, fts ++ ts.toSet)
      }
      val e = toEnumeration(assimilationPath)
      TableSet(Table(
        name = subTypeParent.name,
        description = assimilationPath.tipAssimilationReference.assimilationReference.description.map(toTableDescription),
        columns = subTypeParent.primaryKeys ++ parent.foreignKeys ++
          {
            Column(
              name = assimilationPath.tipAssimilationReference.assimilationReference.name.getOrElse(toColumnName(s"${assimilationPath.tipAssimilationReference.dataType.name()}_TYPE") + "_CD"),
              description = assimilationPath.tipAssimilationReference.assimilationReference.description,
              `type` = ColumnType.VariableCharacter(e.maximumValueSize),
              enumeration = Some(e),
              assimilationPath = assimilationPath) +: consumedColumns
          },
        assimilationPath = assimilationPath), childTables)
    } else throw new IllegalStateException(s"It is illegal for assimililation '${assimilationPath.tipAssimilationReference.assimilationReference.filePath}' to have no data type references.")
  }

  def toEnumeration(assimilationPath: AssimilationPath[AssimilationReference])(implicit config: Config, model: Model) =
    Enumeration(toEnumerationName(assimilationPath.tipName),
      assimilationPath.tipDataTypes.map(dt => EnumerationValue(toEnumerationValueName(dt.name()), Some(dt.description()))))

  def createTableInformation(assimilationPath: AssimilationPath[_], foreignPrimaryKeys: Seq[Column] = Seq()) = {
    val tableName = toTableName(assimilationPath.tipName)
    val primaryKey = Column(tableName + "_SK", Some(s"Surrogate Key (system generated) for the ${tableName} table."), ColumnType.Number(15, 0), assimilationPath, true)
    TableInformation(tableName, if (foreignPrimaryKeys.isEmpty) Seq(primaryKey) else foreignPrimaryKeys)
  }

  def toTableFromDataType(assimilationPath: AssimilationPath[DataType], parent: Option[TableInformation] = None, definingRelationship: Boolean = false)(implicit config: Config, model: Model): TableSet = {
    val ti = createTableInformation(assimilationPath, if (definingRelationship) parent.get.primaryForeignKeys else Seq())
    val (columns, childTables) = assimilationPath.tipDataType.assimilationReferences.map(ar => columnsAndChildTables(ti, assimilationPath + ar)).foldLeft((Seq.empty[Column], Set.empty[Table])) {
      case ((fcs, fts), (cs, ts)) => (fcs ++ cs, fts ++ ts)
    }
    TableSet(Table(
      ti.name,
      assimilationPath.tipDescription.map(toTableDescription),
      ti.primaryKeys ++ { if (definingRelationship || !parent.isDefined) Seq() else parent.get.foreignKeys } ++ columns,
      assimilationPath), childTables)
  }

}