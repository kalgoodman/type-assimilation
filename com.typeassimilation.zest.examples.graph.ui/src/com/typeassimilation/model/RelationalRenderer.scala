package com.typeassimilation.model

import Model.Implicits._

object RelationalRenderer {
  sealed trait ColumnType
  object ColumnType {
    case object Date extends ColumnType
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
  case class Table(name: String, description: Option[String], columns: Seq[Column], sourceDataType: Option[DataType] = None) {
    private val nameToColumnMap = columns.map(c => c.name -> c).toMap
    def column(name: String) = nameToColumnMap.get(name)
    def primaryKeys = columns.filter(_.isPrimaryKey)
    def foreignKeys = columns.filter(_.foreignKeyReference.isDefined)
    override def toString = name + (if (description.isDefined) s" - ${description.get}" else "") + " {\n" + columns.map(c => s"\t${c.toString}").mkString("\n") + "\n}\n"
  }
  case class SourceAssimilation(dataType: DataType, assimilationReference: AssimilationReference) {
    def assimilationOption(implicit model: Model) = model.assimilationOption(assimilationReference.filePath.toAbsoluteUsingBase(dataType.filePath.parent.get))
    def assimilation(implicit model: Model) = assimilationOption.get
  }
  case class Column(name: String, description: Option[String], `type`: ColumnType, isPrimaryKey: Boolean = false, enumeration: Option[Enumeration] = None, foreignKeyReference: Option[ColumnReference] = None, sourceAssimilation: Option[SourceAssimilation] = None) {
    override def toString = s"$name " + `type`.toString + (if (enumeration.isDefined) s" (ENUM: ${enumeration.get.name})" else "") + {
      if (isPrimaryKey && foreignKeyReference.isEmpty) "(PK)"
      else if (!isPrimaryKey && foreignKeyReference.isDefined) "(FK -> " + foreignKeyReference.get + ")"
      else if (isPrimaryKey && foreignKeyReference.isDefined) "(PFK -> " + foreignKeyReference.get + ")"
      else ""
    } + (if (description.isDefined) s" - ${description.get}" else "")
    def toForeignKeyReference(tableName: String) = ColumnReference(tableName, name)
  }
  case class ColumnReference(tableName: String, columnName: String) {
    override def toString = s"$tableName.$columnName"
  }
  case class Enumeration(name: String, values: Seq[EnumerationValue]) {
    def maximumValueSize = values.map(_.name.length).foldLeft(0)((size, v) => if (v > size) v else size)
    override def toString =  s"$name {\n" + values.map(v => s"\t${v.toString}").mkString("\n") + "\n}\n"
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
    maximumSubTypeColumnsBeforeSplittingOut: Int = 3)

  def render(implicit model: Model, config: Config = Config()): RelationalModel = {
    RelationalModel(model.rootDataTypes.flatMap(toTable(_)).toSet)
  }

  def toTableName(dataTypeName: String) = dataTypeName.trim.replaceAll("\\s+", "_").toUpperCase
  def toTableDescription(dataTypeDescription: String) = dataTypeDescription
  def toForeignKeyDescription(primaryKeyDescription: String) = s"Foreign key to ${primaryKeyDescription}"
  def toColumnName(assimilationName: String) = toTableName(assimilationName)
  def toEnumerationName(enumerationName: String) = toTableName(enumerationName)
  def toEnumerationValueName(enumerationValueName: String) = toEnumerationName(enumerationValueName)

  def toPresetColumns(currentTableName: String, sourceAssimilation: SourceAssimilation)(implicit config: Config, model: Model): Seq[Column] = {
    def column(columnType: ColumnType, suffix: String = "") = Column(
      name = sourceAssimilation.assimilationReference.name.map(toColumnName).getOrElse(sourceAssimilation.assimilation.dataTypes.head.name()) + suffix,
      description = sourceAssimilation.assimilationReference.description,
      `type` = columnType,
      isPrimaryKey = false,
      sourceAssimilation = Some(sourceAssimilation))
    import Preset.Assimilation._
    sourceAssimilation.assimilation match {
      case Identifier => Seq(column(ColumnType.VariableCharacter(config.identifierLength)))
      case DateTime => Seq(column(ColumnType.DateTime))
      case Date => Seq(column(ColumnType.Date))
      case Money => Seq(column(ColumnType.Number(5, 3), config.moneyAmountSuffix), column(ColumnType.FixedCharacter(3), config.moneyCurrencyCodeSuffix))
      case IntegralNumber => Seq(column(ColumnType.Integer))
      case DecimalNumber => Seq(column(ColumnType.Number(10, 5)))
      case Boolean => Seq(column(ColumnType.FixedCharacter(1)))
      case CharacterString => Seq(column(ColumnType.VariableCharacter(500)))
    }
  }

  case class TableInformation(name: String, primaryKeys: Seq[Column]) {
    def foreignKeyReferences = primaryKeys.map(pk => ColumnReference(name, pk.name))
  }

  def columnsAndChildTables(currentTableInformation: TableInformation, sourceAssimilation: SourceAssimilation)(implicit config: Config, model: Model): (Seq[Column], Set[Table]) = {
    def isNonGeneratedColumn(c: Column) = !c.isPrimaryKey && c.foreignKeyReference.map(_.tableName != currentTableInformation.name).getOrElse(true)
    def consumeColumns(cs: Seq[Column]) = cs.map(c => c.copy(name = toColumnName(s"${sourceAssimilation.assimilationReference.name.getOrElse("")} ${c.name}")))
    val maxOccurs = sourceAssimilation.assimilationReference.maximumOccurences

    if (maxOccurs == None || maxOccurs.get > config.maximumOccurencesTableLimit) (Seq(), toTable(sourceAssimilation, currentTableInformation))
    else if (maxOccurs.get >= 1) {
      val tables = toTable(sourceAssimilation, currentTableInformation)
      if (tables.size == 1) {
        val comsumedColumns = consumeColumns(tables.head.columns.filter(isNonGeneratedColumn))
        if (maxOccurs.get == 1) (comsumedColumns, Set())
        else (for {
          i <- (1 to (maxOccurs.get))
          c <- comsumedColumns
        } yield c.copy(name = s"${c.name}_$i"), Set())
      } else (Seq(), tables)
    } else (Seq(), Set()) // Bizarre case where maxOccurences is set to zero?
  }
  
  def toTable(sourceAssimilation: SourceAssimilation, parent: TableInformation)(implicit config: Config, model: Model): Set[Table] = {
    def sourceAssimilationAsTable(dataType: DataType) = toTable(
      dataType,
      Some(sourceAssimilation.assimilationReference.name.getOrElse(s"${parent.name} ${dataType.name()}")),
      sourceAssimilation.assimilationReference.description.orElse(Some(dataType.description())),
      Some(parent))
    
    def isNonGeneratedColumn(c: Column) = !c.isPrimaryKey && c.foreignKeyReference.map(_.tableName != parent.name).getOrElse(true)
    def consumeColumns(cs: Seq[Column]) = cs.map(c => c.copy(name = toColumnName(s"${sourceAssimilation.assimilationReference.name.getOrElse("")} ${c.name}")))
    
    if (sourceAssimilation.assimilation.isPreset) {
      val ti = createTableInformation(sourceAssimilation.assimilationReference.name.getOrElse(s"${sourceAssimilation.dataType.name} Type"))
      Set(Table(
        name = ti.name,
        description = sourceAssimilation.assimilationReference.description.map(toTableDescription),
        columns = ti.primaryKeys ++ parent.primaryKeys.map(pk => toForeignKey(parent.name, pk)) ++ toPresetColumns(ti.name, sourceAssimilation)
      ))
    }
    else if (sourceAssimilation.assimilation.dataTypeReferences.size == 1) sourceAssimilationAsTable(sourceAssimilation.assimilation.singleDataType)
    else  if (sourceAssimilation.assimilation.dataTypeReferences.size > 1) {
      val (consumedColumns, childTables) = sourceAssimilation.assimilation.dataTypes.map(sourceAssimilationAsTable).foldLeft((Seq.empty[Column], Set.empty[Table])) {
        case ((fcs, fts), ts) => if (ts.size == 1 && ts.head.columns.filter(isNonGeneratedColumn).size <= config.maximumSubTypeColumnsBeforeSplittingOut) (fcs ++ consumeColumns(ts.head.columns.filter(isNonGeneratedColumn)), fts) else (fcs, fts ++ ts)
      }
      val e = toEnumeration(sourceAssimilation)
      val ti = createTableInformation(sourceAssimilation.assimilationReference.name.getOrElse(s"${sourceAssimilation.dataType.name()} Type"))
      
      childTables + Table(
        name = ti.name,
        description = sourceAssimilation.assimilationReference.description.map(toTableDescription),
        columns = ti.primaryKeys ++ parent.primaryKeys.map(pk => toForeignKey(parent.name, pk)) ++ 
        {Column(
          name = sourceAssimilation.assimilationReference.name.getOrElse(toColumnName(s"${sourceAssimilation.dataType.name()}_TYPE") + "_CD"),
          description = sourceAssimilation.assimilationReference.description,
          `type` = ColumnType.VariableCharacter(e.maximumValueSize),
          enumeration = Some(e)
        ) +: consumedColumns}
      ) 
    }
    else throw new IllegalStateException(s"It is illegal for assimililation '${sourceAssimilation.assimilationReference.filePath}'' to have no data type references.")
  }

  def toEnumeration(sourceAssimilation: SourceAssimilation)(implicit config: Config, model: Model) =
    Enumeration(sourceAssimilation.assimilationReference.name.getOrElse(toEnumerationName(s"${sourceAssimilation.dataType.name()} Type")),
      sourceAssimilation.assimilation.dataTypes.map(dt => EnumerationValue(toEnumerationValueName(dt.name()), Some(dt.description()))))

  def toForeignKey(parentTableName: String, primaryKeyColumn: Column) = Column(
      name = primaryKeyColumn.name,
      description = primaryKeyColumn.description.map(toForeignKeyDescription),
      `type` = primaryKeyColumn.`type`,
      foreignKeyReference = Some(primaryKeyColumn.toForeignKeyReference(parentTableName)))
  
  def createTableInformation(conceptualName: String) = {
    val tableName = toTableName(conceptualName)
    val primaryKey = Column(tableName + "_SK", Some(s"Surrogate Key (system generated) for the ${tableName} table."), ColumnType.Number(15, 0), true)
    TableInformation(tableName, Seq(primaryKey))
  }    
      
  def toTable(dataType: DataType, name: Option[String] = None, description: Option[String] = None, parent: Option[TableInformation] = None)(implicit config: Config, model: Model): Set[Table] = {
    val ti = createTableInformation(name.getOrElse(dataType.name()))
    val (columns, childTables) = dataType.assimilationReferences.map(ar => columnsAndChildTables(ti, SourceAssimilation(dataType, ar))).foldLeft((Seq.empty[Column], Set.empty[Table])) {
      case ((fcs, fts), (cs, ts)) => (fcs ++ cs, fts ++ ts)
    }
    def parentForeignKeyColumns = for {
      parentTableInformation <- parent.toSeq
      pc <- parentTableInformation.primaryKeys
    } yield toForeignKey(parentTableInformation.name, pc)
    Set(Table(
      ti.name,
      Some(toTableDescription(description.getOrElse(dataType.description()))),
      ti.primaryKeys ++ parentForeignKeyColumns ++ columns,
      Some(dataType))) ++ childTables
  }

}