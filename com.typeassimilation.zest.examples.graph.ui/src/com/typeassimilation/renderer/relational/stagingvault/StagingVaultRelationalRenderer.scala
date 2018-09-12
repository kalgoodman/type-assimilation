package com.typeassimilation.renderer.relational.stagingvault

import com.typeassimilation.model.{ Model => CoreModel } 
import com.typeassimilation.renderer.relational
import com.typeassimilation.renderer.relational.RelationalRenderer.PrimaryKeyPolicy
import com.typeassimilation.renderer.relational.RelationalRenderer.VersioningPolicy
import com.typeassimilation.renderer.relational.RelationalRenderer
import com.typeassimilation.model.AssimilationPathUtils
import com.typeassimilation.renderer.relational.RelationalRenderer.LogicalTable

object StagingVault {
  case class Model(staging: Staging.Model)
  trait Table {
    def relationalTable: relational.Table
    lazy val feed = AssimilationPathUtils.name(relationalTable.assimilationPath.relativeToLastEffectiveOrientatingDataType.commonHead.tipEither)
  }
}

object Staging {
  case class Table private[Staging](relationalTable: relational.Table) extends StagingVault.Table
  case class Model private(underlyingModel: relational.RelationalModel) {
    lazy val tables = underlyingModel.tables.map(Table(_))
  }
  object Model {
    def apply(model: CoreModel, baseConfig: RelationalRenderer.Config): Model = Model(relational.RelationalModel(relational.RelationalRenderer.render(model, baseConfig.copy(primaryKeyPolicy = PrimaryKeyPolicy.NaturalKey(), versioningPolicy = VersioningPolicy.NearestOrientating(tieVersions = false)))))
  }
}

object Vault {
  case class Table private[Vault](relationalTable: relational.Table) extends StagingVault.Table
  case class Model private(underlyingModel: relational.RelationalModel) {
    lazy val tables = {
      val initialVaultTables = underlyingModel.tables.map(Table(_))
    }
  }
  object Model {
    def apply(model: CoreModel, baseConfig: RelationalRenderer.Config): Model = {
      val vaultNamingPolicy = new RelationalRenderer.NamingPolicy.Default {
        override def tableName(logicalTable: LogicalTable)(implicit config: RelationalRenderer.Config) = super.tableName(logicalTable) + (if (logicalTable.assimilationPath.hasEffectivelyOrientatingTip) "_VERSION" else "")
      }
      Model(relational.RelationalModel(relational.RelationalRenderer.render(model, baseConfig.copy(namingPolicy = vaultNamingPolicy, primaryKeyPolicy = PrimaryKeyPolicy.SurrogateKeyGeneration, versioningPolicy = VersioningPolicy.NearestOrientating(applyToPk = false)))))
    }
  }
}

object StagingVaultRelationalRenderer {
  def render(model: CoreModel, relationalConfig: RelationalRenderer.Config): StagingVault.Model = {
    val staging = Staging.Model(model, relationalConfig)
    val vault = Vault.Model(model, relationalConfig)
    println(staging)
    println(vault)
    
    ???
  }
  // Render staging
  // Render vault
  // Detect cross feed FKs
  // Add in required timeless tables
  // Redirect foreign keys
}