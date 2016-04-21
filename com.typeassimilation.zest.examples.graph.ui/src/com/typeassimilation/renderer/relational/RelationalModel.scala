package com.typeassimilation.renderer.relational

import com.typeassimilation.model._
import com.typeassimilation.renderer.relational.RelationalRenderer.LogicalRelationalModel
import com.typeassimilation.renderer.relational.RelationalRenderer.LogicalTable
import com.typeassimilation.renderer.relational.RelationalRenderer.ColumnGroup
import com.typeassimilation.renderer.relational.RelationalRenderer.Config

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

object RelationalModel {
  def apply(logicalRelationalModel: LogicalRelationalModel): RelationalModel = RelationalModel(logicalRelationalModel.tables.map(t => table(t, logicalRelationalModel)))
  def primaryKeys(logicalTable: LogicalTable, logicalRelationalModel: LogicalRelationalModel) = {
    implicit val config = logicalRelationalModel.config
    config.versioningPolicy.modifyPrimaryKeys(
      config.primaryKeyPolicy.modifyPrimaryKeys(
        logicalTable.columnGroups.filter(cg => config.primaryKeyPolicy.isPrimaryKeyColumnGroup(cg, logicalTable, logicalRelationalModel)).flatMap(cg => columns(cg, logicalTable, logicalRelationalModel)).map(_.copy(isPrimaryKey = true)),
        logicalRelationalModel.config.namingPolicy.tableName(logicalTable),
        logicalTable,
        logicalRelationalModel),
      logicalTable, logicalRelationalModel)
  }
  case class TableDescriptor(name: String, primaryKeys: Seq[Column])
  def tableDescriptor(logicalTable: LogicalTable, logicalRelationalModel: LogicalRelationalModel): TableDescriptor = TableDescriptor(tableName(logicalTable, logicalRelationalModel), primaryKeys(logicalTable, logicalRelationalModel))
  def tableName(logicalTable: LogicalTable, logicalRelationalModel: LogicalRelationalModel) = logicalRelationalModel.config.namingPolicy.tableName(logicalTable)(logicalRelationalModel.config)
  def logicalTable(table: String, logicalRelationalModel: LogicalRelationalModel): Option[LogicalTable] = logicalRelationalModel.tables.find(t => tableName(t, logicalRelationalModel) == table)
  def sourceLogicalTables(columnReference: ColumnReference, logicalRelationalModel: LogicalRelationalModel): Seq[LogicalTable] = {
    def recurse(cr: ColumnReference): Seq[LogicalTable] = 
      logicalTable(cr.tableName, logicalRelationalModel) match {
        case None => Seq()
        case Some(lt) => tableDescriptor(lt, logicalRelationalModel).primaryKeys.find(_.name == cr.columnName) match {
          case None => Seq()
          case Some(pk) => pk.foreignKeyReference match {
            case None => Seq(lt)
            case Some(nextCr) => lt +: recurse(nextCr)
          }
        }
      }
    recurse(columnReference)
  }
  def table(logicalTable: LogicalTable, logicalRelationalModel: LogicalRelationalModel): Table = {
    implicit val config = logicalRelationalModel.config
    val descriptor = tableDescriptor(logicalTable, logicalRelationalModel)
    Table(
      name = descriptor.name,
      description = config.namingPolicy.tableDescription(logicalTable),
      columns = descriptor.primaryKeys ++
        config.versioningPolicy.modifyNonPrimaryKeys(
            logicalTable.columnGroups.filterNot(cg => config.primaryKeyPolicy.isPrimaryKeyColumnGroup(cg, logicalTable, logicalRelationalModel)).flatMap(cg => columns(cg, logicalTable, logicalRelationalModel)),
        logicalTable, logicalRelationalModel),
      assimilationPath = logicalTable.assimilationPath)
  }
  def columns(columnGroup: ColumnGroup, parentLogicalTable: LogicalTable, logicalRelationalModel: LogicalRelationalModel): Seq[Column] = {
    implicit val config = logicalRelationalModel.config
    def nullable = columnGroup.assimilationPath != parentLogicalTable.assimilationPath && (columnGroup.assimilationPath.relativeTo(parentLogicalTable.assimilationPath) match {
      case None => true
      case Some(jap) =>
        jap.commonAssimilationPath.toSeq.flatMap { case at:BrokenAssimilationPath.AssimilationTip => Some(at.multiplicityRangeLimits.inclusiveLowerBound); case _ => None}.min < 1
    })
    import ColumnGroup._
    columnGroup match {
      case Repeated(columnGroup, repeats) => (1 to repeats).map(AssimilationPath.MultiplicityRange.forOnly).flatMap(newRange => columns(columnGroup.withAssimilationPath(columnGroup.assimilationPath match {
        case at: JoinedAssimilationPath.AssimilationTip => at.withRange(newRange)
        case dtt: JoinedAssimilationPath.DataTypeTip if dtt.parents.size == 1 => dtt.parents.head.withRange(newRange) + dtt.tip
        case ap => throw new UnsupportedOperationException(s"Cannot repeat ColumnGroup with AssimilationPath: $ap")
      }), parentLogicalTable, logicalRelationalModel))
      case sc: SimpleColumn => Seq(Column(
        name = config.namingPolicy.columnName(parentLogicalTable, sc.assimilationPath),
        description = config.namingPolicy.columnDescription(parentLogicalTable, sc.assimilationPath),
        `type` = sc.columnType,
        isGenerated = false,
        isNullable = nullable,
        assimilationPath = sc.assimilationPath))
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
        columns(SimpleColumn(e.assimilationPath, ColumnType(config.enumerationColumnType.replaceAllLiterally("[SIZE]", enumeration.maximumValueSize.toString))), parentLogicalTable, logicalRelationalModel).map(_.copy(
          enumeration = Some(enumeration)))
      case nt: NestedTable => nt.columnGroups.flatMap(cg => columns(cg, parentLogicalTable, logicalRelationalModel))
      case cr: ChildReference =>
        val referencedTable = tableDescriptor(logicalRelationalModel.follow(cr), logicalRelationalModel)
        val parentTables: Set[LogicalTable] = if (cr.isReferenceToWeakAssimilationWithCommonParent) {
          val parentTd = tableDescriptor(parentLogicalTable, logicalRelationalModel)
          parentTd.primaryKeys.flatMap(pk => sourceLogicalTables(pk.toForeignKeyReference(parentTd.name), logicalRelationalModel)).toSet + parentLogicalTable
        } else Set()
        config.versioningPolicy.modifyChildForeignKeys(cr, referencedTable.primaryKeys.filterNot(pk => cr.isReferenceToWeakAssimilationWithCommonParent &&
            !((parentTables & sourceLogicalTables(pk.toForeignKeyReference(referencedTable.name), logicalRelationalModel).toSet).isEmpty)
          ).map(pk => Column(
            name = config.namingPolicy.foreignKeyColumnName(parentLogicalTable, cr.assimilationPath, pk),
            description = config.namingPolicy.foreignKeyColumnDescription(parentLogicalTable, cr.assimilationPath, pk),
            `type` = pk.`type`,
            isGenerated = pk.isGenerated,
            isNullable = nullable,
            foreignKeyReference = Some(pk.toForeignKeyReference(referencedTable.name)),
            assimilationPath = cr.assimilationPath)), parentLogicalTable, logicalRelationalModel)
      case pr: ParentReference =>
        val referencedTable = tableDescriptor(logicalRelationalModel.follow(pr), logicalRelationalModel)
        referencedTable.primaryKeys.map(pk => Column(
          name = config.namingPolicy.parentForeignKeyColumnName(parentLogicalTable, pr.assimilationPath, pk),
          description = config.namingPolicy.parentForeignKeyColumnDescription(parentLogicalTable, pr.assimilationPath, pk),
          `type` = pk.`type`,
          isGenerated = pk.isGenerated,
          isNullable = nullable,
          foreignKeyReference = Some(pk.toForeignKeyReference(referencedTable.name)),
          assimilationPath = pr.assimilationPath))
    }
  }

}

case class ColumnType(typeDefinition: String)
case class Table(name: String, description: Option[String], columns: Seq[Column], assimilationPath: JoinedAssimilationPath) {
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
case class Column(name: String, description: Option[String], `type`: ColumnType, isGenerated: Boolean, isNullable: Boolean, assimilationPath: JoinedAssimilationPath, isPrimaryKey: Boolean = false, enumeration: Option[Enumeration] = None, foreignKeyReference: Option[ColumnReference] = None) {
  def isIdentifying(logicalTable: LogicalTable) = assimilationPath.relativeTo(logicalTable.assimilationPath).get.commonHead match {
    case at: AssimilationPath.AssimilationTip => at.tip.assimilation.isIdentifying
    case _ => false
  }
  override def toString = s"$name " + `type`.typeDefinition + (if (enumeration.isDefined) s" (ENUM: ${enumeration.get.name})" else "") + {
    if (isPrimaryKey && foreignKeyReference.isEmpty) " (PK)"
    else if (!isPrimaryKey && foreignKeyReference.isDefined) " (FK -> " + foreignKeyReference.get + ")"
    else if (isPrimaryKey && foreignKeyReference.isDefined) " (PFK -> " + foreignKeyReference.get + ")"
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
