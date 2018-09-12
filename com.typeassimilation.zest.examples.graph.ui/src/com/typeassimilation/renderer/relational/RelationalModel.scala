package com.typeassimilation.renderer.relational

import com.typeassimilation.model._
import com.typeassimilation.renderer.relational.RelationalRenderer.LogicalRelationalModel
import com.typeassimilation.renderer.relational.RelationalRenderer.LogicalTable
import com.typeassimilation.renderer.relational.RelationalRenderer.ColumnGroup
import com.typeassimilation.renderer.relational.RelationalRenderer.Config

class RelationalModel(definedTables: Set[Table]) {
  lazy val tables = definedTables.map(_.copy(modelOption = Some(this)))
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

object RelationalModel {
  def apply(logicalRelationalModel: LogicalRelationalModel): RelationalModel = new RelationalModel(logicalRelationalModel.tables.map(table))
  def primaryKeys(logicalTable: LogicalTable) = {
    implicit val config = logicalTable.model.config
    config.versioningPolicy.modifyPrimaryKeys(
      config.primaryKeyPolicy.modifyPrimaryKeys(
        logicalTable.columnGroups.filter(cg => config.primaryKeyPolicy.isPrimaryKeyColumnGroup(cg, logicalTable)).flatMap(cg => columns(cg, logicalTable)).map(_.copy(isPrimaryKey = true)),
        config.namingPolicy.tableName(logicalTable),
        logicalTable),
      logicalTable)
  }
  case class TableDescriptor(name: String, primaryKeys: Seq[Column])
  def tableDescriptor(logicalTable: LogicalTable): TableDescriptor = TableDescriptor(tableName(logicalTable), primaryKeys(logicalTable))
  def tableName(logicalTable: LogicalTable) = logicalTable.model.config.namingPolicy.tableName(logicalTable)(logicalTable.model.config)
  def logicalTable(table: String, logicalRelationalModel: LogicalRelationalModel): Option[LogicalTable] = logicalRelationalModel.tables.find(t => tableName(t) == table)
  def sourceLogicalTables(columnReference: ColumnReference, logicalRelationalModel: LogicalRelationalModel): Seq[LogicalTable] = {
    def recurse(cr: ColumnReference): Seq[LogicalTable] = 
      logicalTable(cr.tableName, logicalRelationalModel) match {
        case None => Seq()
        case Some(lt) => tableDescriptor(lt).primaryKeys.find(_.name == cr.columnName) match {
          case None => Seq()
          case Some(pk) => if(pk.foreignKeyReferences.isEmpty) Seq(lt) else lt +: pk.foreignKeyReferences.toSeq.flatMap(recurse)
        }
      }
    recurse(columnReference)
  }
  def table(logicalTable: LogicalTable): Table = {
    implicit val config = logicalTable.model.config
    val descriptor = tableDescriptor(logicalTable)
    val (nonPkColumns, foreignKeys) = logicalTable.columnGroups.filterNot(cg => config.primaryKeyPolicy.isPrimaryKeyColumnGroup(cg, logicalTable)).foldLeft((Seq.empty[Column], Set.empty[ForeignKey])) {
        case((columns, foreignKeys), cg) =>  
          val (newColumns, newForeignKeys) = columnsAndForeignKeys(cg, logicalTable)
          (columns ++ newColumns, foreignKeys ++ newForeignKeys)
    }
    Table(
      name = descriptor.name,
      descriptionOption = config.namingPolicy.tableDescription(logicalTable),
      columns = descriptor.primaryKeys ++
        config.versioningPolicy.modifyNonPrimaryKeys(nonPkColumns, logicalTable),
      foreignKeys = foreignKeys,
      assimilationPath = logicalTable.assimilationPath)
  }
  def columnsAndForeignKeys(columnGroup: ColumnGroup, parentLogicalTable: LogicalTable): (Seq[Column], Set[ForeignKey]) = {
    implicit val config = parentLogicalTable.model.config
    def nullable = columnGroup.assimilationPath != parentLogicalTable.assimilationPath && (columnGroup.assimilationPath.relativeTo(parentLogicalTable.assimilationPath) match {
      case None => true
      case Some(jap) =>
        jap.commonAssimilationPath.toSeq.flatMap { case at:BrokenAssimilationPath.AssimilationTip => Some(at.multiplicityRangeLimits.inclusiveLowerBound); case _ => None}.min < 1
    })
    import ColumnGroup._
    columnGroup match {
      case Repeated(columnGroup, repeats) => (1 to repeats).map(AssimilationPath.MultiplicityRange.forOnly).foldLeft((Seq.empty[Column], Set.empty[ForeignKey])) {
        case((columns, foreignKeys), newRange) => 
        val (newColumns, newForeignKeys) = columnsAndForeignKeys(columnGroup.withAssimilationPath(columnGroup.assimilationPath match {
        case at: JoinedAssimilationPath.AssimilationTip => at.withRange(newRange)
        case dtt: JoinedAssimilationPath.DataTypeTip if dtt.parents.size == 1 => dtt.parents.head.withRange(newRange) + dtt.tip
        case ap => throw new UnsupportedOperationException(s"Cannot repeat ColumnGroup with AssimilationPath: $ap")
      }), parentLogicalTable)
      
      (columns ++ newColumns, foreignKeys ++ newForeignKeys)}
      case sc: SimpleColumn => (Seq(Column(
        name = config.namingPolicy.columnName(parentLogicalTable, sc.assimilationPath),
        description = config.namingPolicy.columnDescription(parentLogicalTable, sc.assimilationPath),
        `type` = sc.columnType,
        isGenerated = false,
        isNullable = nullable,
        assimilationPath = sc.assimilationPath)), Set())
      case e: EnumerationColumn =>
        val enumeration = {
          Enumeration(
            name = config.namingPolicy.enumerationName(e.assimilationPath),
            values = e.assimilationPath.tip.dataTypes.map {
              dt =>
                val valueAssimilationPath = e.assimilationPath + dt
                EnumerationValue(config.namingPolicy.enumerationValueName(valueAssimilationPath), config.namingPolicy.enumerationValueDescription(valueAssimilationPath), valueAssimilationPath)
            },
            assimilationPath = e.assimilationPath)
        }
        val (newColumns, newForeignKeys) = columnsAndForeignKeys(SimpleColumn(e.assimilationPath, ColumnType(config.enumerationColumnType.replaceAllLiterally("[SIZE]", enumeration.maximumValueSize.toString))), parentLogicalTable)
        (newColumns.map(_.copy(enumeration = Some(enumeration))), newForeignKeys)
      case nt: NestedTable => nt.columnGroups.foldLeft((Seq.empty[Column], Set.empty[ForeignKey])) {
        case((columns, foreignKeys), cg)  => val (newColumns, newForeignKeys) = columnsAndForeignKeys(cg, parentLogicalTable)
        (columns ++ newColumns, foreignKeys ++ newForeignKeys)
      }
      case cr: ChildReference =>
        val referencedTable = tableDescriptor(parentLogicalTable.model.follow(cr))
        val parentTables: Set[LogicalTable] = if (cr.isReferenceToWeakAssimilationWithCommonParent) {
          val parentTd = tableDescriptor(parentLogicalTable)
          parentTd.primaryKeys.flatMap(pk => sourceLogicalTables(pk.toForeignKeyReference(parentTd.name), parentLogicalTable.model)).toSet + parentLogicalTable
        } else Set()
        config.versioningPolicy.modifyChildForeignKeys(cr, referencedTable.primaryKeys.filterNot(pk => cr.isReferenceToWeakAssimilationWithCommonParent &&
            !((parentTables & sourceLogicalTables(pk.toForeignKeyReference(referencedTable.name), parentLogicalTable.model).toSet).isEmpty)
          ).map(pk => Column(
            name = config.namingPolicy.foreignKeyColumnName(parentLogicalTable, cr.assimilationPath, pk),
            description = config.namingPolicy.foreignKeyColumnDescription(parentLogicalTable, cr.assimilationPath, pk),
            `type` = pk.`type`,
            isGenerated = pk.isGenerated,
            isNullable = nullable,
//            foreignKeyReferences = Some(pk.toForeignKeyReference(referencedTable.name)),
            assimilationPath = cr.assimilationPath)), parentLogicalTable)
      case pr: ParentReference =>
        val referencedTable = tableDescriptor(parentLogicalTable.model.follow(pr))
        referencedTable.primaryKeys.map(pk => Column(
          name = config.namingPolicy.parentForeignKeyColumnName(parentLogicalTable, pr.assimilationPath, pk),
          description = config.namingPolicy.parentForeignKeyColumnDescription(parentLogicalTable, pr.assimilationPath, pk),
          `type` = pk.`type`,
          isGenerated = pk.isGenerated,
          isNullable = nullable,
//          foreignKeyReference = Some(pk.toForeignKeyReference(referencedTable.name)),
          assimilationPath = pr.assimilationPath))
    }
  }

}

case class ColumnType(typeDefinition: String)
case class ForeignKey(targetTableName: String, columnNameMappings: Seq[(String, String)], tableOption: Option[Table] = None) {
  def table = tableOption.getOrElse(throw new IllegalStateException(s"$this has not been associated with a table yet."))
  lazy val targetTable = table.model.table(targetTableName).
      getOrElse(throw new IllegalStateException(s"Target table $targetTableName does not exist in this model"))
  lazy val columnMappings = columnNameMappings.map { case (sourceColumnName, targetColumnName) => 
    table.column(sourceColumnName).
      getOrElse(throw new IllegalStateException(s"Source column $sourceColumnName does not exist on ${table.name}")) ->
    targetTable.column(targetColumnName).
      getOrElse(throw new IllegalStateException(s"Target column $targetColumnName does not exist on ${table.name}"))}
  lazy val columnMap = columnMappings.toMap
  lazy val columnNameMap = columnNameMappings.toMap
  def sourceColumns = columnMappings.map(_._1)
  def targetColumns = columnMappings.map(_._2)
}
class Table(val name: String, val descriptionOption: Option[String], definedColumns: Seq[Column], val assimilationPath: JoinedAssimilationPath, definedForeignKeys: Set[ForeignKey] = Set(), modelOption: Option[RelationalModel] = None) {
  lazy val columns = definedColumns.map(_.copy(tableOption = Some(this)))
  lazy val foreignKeys = definedForeignKeys.map(_.copy(tableOption = Some(this)))
  def model = modelOption.getOrElse(throw new IllegalStateException(s"$this has not been associated with a model yet."))
  private val nameToColumnMap = columns.map(c => c.name -> c).toMap
  def column(name: String) = nameToColumnMap.get(name)
  def primaryKeys = columns.filter(_.isPrimaryKey)
  def primaryForeignKeys = primaryKeys.filter(!_.foreignKeyReferences.isEmpty)
  def nonKeyColumns = columns.filter(c => !c.isPrimaryKey && c.foreignKeyReferences.isEmpty)
  def validate = {
    val repeatedColumns = columns.groupBy(_.name).mapValues(_.size).filter {
      case (name, size) => size > 1
    }
    if (!repeatedColumns.isEmpty) throw new IllegalStateException(s"Table contains repeated column names: $this")
  }
  def copy(name: String = this.name,
      descriptionOption: Option[String] = this.descriptionOption,
      definedColumns: Seq[Column] = this.definedColumns,
      assimilationPath: JoinedAssimilationPath = this.assimilationPath,
      definedForeignKeys: Set[ForeignKey] = this.definedForeignKeys,
      modelOption: Option[RelationalModel] = this.modelOption) = new Table(name, descriptionOption, definedColumns, assimilationPath, definedForeignKeys, modelOption)
  override def toString = name + (if (descriptionOption.isDefined) s": ${descriptionOption.get}" else "") + s" [$assimilationPath] {\n" + columns.map(c => s"\t${c.toString}").mkString("\n") + "\n}\n"
}
object Table {
  def apply(name: String, descriptionOption: Option[String], columns: Seq[Column], assimilationPath: JoinedAssimilationPath, foreignKeys: Set[ForeignKey]) = new Table(name, descriptionOption, columns, assimilationPath, foreignKeys)
}
case class Column(name: String, description: Option[String], `type`: ColumnType, isGenerated: Boolean, isNullable: Boolean, assimilationPath: JoinedAssimilationPath, isPrimaryKey: Boolean = false, enumeration: Option[Enumeration] = None, tableOption: Option[Table] = None) {
  lazy val table = tableOption.getOrElse(throw new IllegalStateException(s"$this has not been added to a table yet."))
  lazy val isIdentifying = isIdentifyingRelativeTo(table.assimilationPath)
  def isIdentifyingRelativeTo(tableAssimilationPath: JoinedAssimilationPath) = assimilationPath.relativeTo(tableAssimilationPath).get.commonHead match {
    case at: AssimilationPath.AssimilationTip => at.tip.assimilation.isIdentifying
    case _ => false
  }
  def foreignKeyReferences = table.foreignKeys.flatMap(fk => fk.columnNameMap.get(name).map(fkColumnName => ColumnReference(fk.targetTableName, fkColumnName)))
  override def toString = s"$name " + `type`.typeDefinition + (if (enumeration.isDefined) s" (ENUM: ${enumeration.get.name})" else "") + {
    if (isPrimaryKey && foreignKeyReferences.isEmpty) " (PK)"
    else if (!isPrimaryKey && !foreignKeyReferences.isEmpty) " (FK -> " + foreignKeyReferences.mkString(", ") + ")"
    else if (isPrimaryKey && !foreignKeyReferences.isEmpty) " (PFK -> " + foreignKeyReferences.mkString(", ") + ")"
    else ""
  } + (if (isNullable) " NULL" else " NOT NULL") + (if (isGenerated) " <GENERATED>" else "") + (if (description.isDefined) s": ${description.get}" else "") + s" [$assimilationPath]"
  def toForeignKeyReference(tableName: String) = ColumnReference(tableName, name)
}
case class ColumnReference(tableName: String, columnName: String) {
  override def toString = s"$tableName.$columnName"
}
case class Enumeration(name: String, values: Seq[EnumerationValue], assimilationPath: JoinedAssimilationPath.AssimilationTip) {
  def maximumValueSize = values.map(_.name.length).foldLeft(0)((size, v) => if (v > size) v else size)
  override def toString = s"$name [$assimilationPath] {\n" + values.map(v => s"\t${v.toString}").mkString("\n") + "\n}\n"
}
case class EnumerationValue(name: String, description: Option[String], assimilationPath: JoinedAssimilationPath.DataTypeTip) {
  override def toString = name + (if (description.isDefined) s" - ${description.get}" else "") + s"  [$assimilationPath]"
}
