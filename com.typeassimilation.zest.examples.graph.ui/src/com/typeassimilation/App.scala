package com.typeassimilation

import java.io.File
import com.typeassimilation.model.ModelPesistence
import com.typeassimilation.renderer.relational.RelationalRenderer
import com.typeassimilation.renderer.relational.RelationalModel
import com.typeassimilation.renderer.relational.RelationalRenderer.Config
import com.typeassimilation.renderer.relational.RelationalRenderer.PrimaryKeyPolicy
import com.typeassimilation.renderer.relational.RelationalRenderer.VersioningPolicy
import com.typeassimilation.renderer.relational.RelationalRendererPersistence
import com.typeassimilation.model.FilePath

object App {
    def main(args: Array[String]): Unit = {
      val rootDirectory = new File(args(0))
      val model = ModelPesistence.readDirectory(rootDirectory)
      val relationalRendererConfig = RelationalRendererPersistence.fromFile(FilePath("/relational.xml").asAbsolute, rootDirectory)
      val logical = RelationalRenderer.render(model, relationalRendererConfig)
      println("******* LOGICAL *******")
      println(logical)
      println()
      println("******* CONCRETE LOGICAL *******")
      println(RelationalModel(logical))
      ModelPesistence.writeDirectory(model, new File(args(1)))
    }
}