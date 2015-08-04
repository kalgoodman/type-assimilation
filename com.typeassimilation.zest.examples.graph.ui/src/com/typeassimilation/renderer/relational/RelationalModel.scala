package com.typeassimilation.renderer.relational

import com.typeassimilation.model._
import com.typeassimilation.renderer.relational.RelationalRenderer.LogicalRelationalModel
import com.typeassimilation.renderer.relational.RelationalRenderer.LogicalRelationalModel
import com.typeassimilation.renderer.relational.RelationalRenderer.LogicalTable
import com.typeassimilation.renderer.relational.RelationalRenderer.ColumnGroup
import com.typeassimilation.renderer.relational.RelationalRenderer.Config
import com.typeassimilation.renderer.relational.RelationalRenderer.LogicalRelationalModel

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
    implicit val model = logicalRelationalModel.model
    implicit val config = logicalRelationalModel.config
    config.primaryKeyPolicy.modifyPrimaryKeys(
       logicalTable.columnGroups.filter(cg => config.primaryKeyPolicy.isPrimaryKeyColumnGroup(cg, logicalTable, logicalRelationalModel)).flatMap(cg => columns(cg, Seq(), logicalTable, logicalRelationalModel)).map(_.copy(isPrimaryKey = true)),
       logicalRelationalModel.config.namingPolicy.tableName(logicalTable),
       logicalTable,
       logicalRelationalModel)
  }
  case class TableDescriptor(name: String, primaryKeys: Seq[Column])
  def tableDescriptor(logicalTable: LogicalTable, logicalRelationalModel: LogicalRelationalModel) = {
    implicit val model = logicalRelationalModel.model
    implicit val config = logicalRelationalModel.config
    TableDescriptor(logicalRelationalModel.config.namingPolicy.tableName(logicalTable), primaryKeys(logicalTable, logicalRelationalModel))
  }
  def table(logicalTable: LogicalTable, logicalRelationalModel: LogicalRelationalModel): Table = {
    implicit val model = logicalRelationalModel.model
    implicit val config = logicalRelationalModel.config
    val descriptor = tableDescriptor(logicalTable, logicalRelationalModel)
    Table(
       name = descriptor.name,
       description = config.namingPolicy.tableDescription(logicalTable),
       columns = descriptor.primaryKeys ++
         logicalTable.columnGroups.filterNot(cg => config.primaryKeyPolicy.isPrimaryKeyColumnGroup(cg, logicalTable, logicalRelationalModel)).flatMap(cg => columns(cg, Seq(), logicalTable, logicalRelationalModel)),
       assimilationPath = logicalTable.assimilationPath
    )
  }
  def columns(columnGroup: ColumnGroup, repeatIndexes: Seq[Int], parentLogicalTable: LogicalTable, logicalRelationalModel: LogicalRelationalModel): Seq[Column] = {
    implicit val model = logicalRelationalModel.model
    implicit val config = logicalRelationalModel.config
    def nullable = columnGroup.assimilationPath != parentLogicalTable.assimilationPath && columnGroup.assimilationPath.relativeTo(parentLogicalTable.assimilationPath).assimilationPath.commonAssimilations.head.assimilation.minimumOccurences.getOrElse(0) < 1
    import ColumnGroup._
    columnGroup match {
      case Repeated(columnGroup, repeats) => (1 to repeats).flatMap(index => columns(columnGroup, repeatIndexes :+ index, parentLogicalTable, logicalRelationalModel))
      case sc: SimpleColumn => Seq(Column(
          name = config.namingPolicy.columnName(parentLogicalTable, sc.assimilationPath, repeatIndexes),
          description = config.namingPolicy.columnDescription(parentLogicalTable, sc.assimilationPath, repeatIndexes),
          `type` = sc.columnType, 
          isGenerated = false,
          isNullable = nullable,
          assimilationPath = sc.assimilationPath
          ))
      case e: EnumerationColumn => 
        val enumeration = {
          Enumeration(
              name = config.namingPolicy.enumerationName(e.assimilationPath),
              values = e.assimilationPath.tipDataTypes.map {
                dt =>
                  val valueAssimilationPath = e.assimilationPath + dt
                  EnumerationValue(config.namingPolicy.enumerationValueName(valueAssimilationPath), config.namingPolicy.enumerationValueDescription(valueAssimilationPath), valueAssimilationPath)
              },
              assimilationPath = e.assimilationPath.asAssimilation)
        }
        columns(SimpleColumn(e.assimilationPath, ColumnType.VariableCharacter(enumeration.maximumValueSize)), repeatIndexes, parentLogicalTable, logicalRelationalModel).map(_.copy(
          enumeration = Some(enumeration)
        ))
      case nt: NestedTable => nt.columnGroups.flatMap(cg => columns(cg, repeatIndexes, parentLogicalTable, logicalRelationalModel)) 
      case cr: ChildReference => 
        val referencedTable = tableDescriptor(logicalRelationalModel.follow(cr), logicalRelationalModel)
        referencedTable.primaryKeys.map(pk => Column(
          name = config.namingPolicy.foreignKeyColumnName(parentLogicalTable, cr.assimilationPath, pk, repeatIndexes),
          description = config.namingPolicy.foreignKeyColumnDescription(parentLogicalTable, cr.assimilationPath, pk, repeatIndexes),
          `type` = pk.`type`,
          isGenerated = pk.isGenerated,
          isNullable = nullable,
          foreignKeyReference = Some(ColumnReference(referencedTable.name, pk.name)),
          assimilationPath = cr.assimilationPath
        ))
      case pr: ParentReference =>
        val referencedTable = tableDescriptor(logicalRelationalModel.follow(pr), logicalRelationalModel)
        referencedTable.primaryKeys.map(pk => Column(
          name = config.namingPolicy.parentForeignKeyColumnName(parentLogicalTable, pr.assimilationPath, pk),
          description = config.namingPolicy.parentForeignKeyColumnDescription(parentLogicalTable, pr.assimilationPath, pk),
          `type` = pk.`type`,
          isGenerated = pk.isGenerated,
          isNullable = nullable,
          foreignKeyReference = Some(ColumnReference(referencedTable.name, pk.name)),
          assimilationPath = pr.assimilationPath
        ))
    }
  }
    
}

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
case class Column(name: String, description: Option[String], `type`: ColumnType, isGenerated: Boolean, isNullable: Boolean, assimilationPath: JoinedAssimilationPath[_], isPrimaryKey: Boolean = false, enumeration: Option[Enumeration] = None, foreignKeyReference: Option[ColumnReference] = None) {
  override def toString = s"$name " + `type`.toString + (if (enumeration.isDefined) s" (ENUM: ${enumeration.get.name})" else "") + {
    if (isPrimaryKey && foreignKeyReference.isEmpty) "(PK)"
    else if (!isPrimaryKey && foreignKeyReference.isDefined) "(FK -> " + foreignKeyReference.get + ")"
    else if (isPrimaryKey && foreignKeyReference.isDefined) "(PFK -> " + foreignKeyReference.get + ")"
    else ""
  } + (if (isNullable) " NULL" else " NOT NULL") + (if (isGenerated) " <GENERATED>" else "") + (if (description.isDefined) s": ${description.get}" else "") + s" [$assimilationPath]"
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
