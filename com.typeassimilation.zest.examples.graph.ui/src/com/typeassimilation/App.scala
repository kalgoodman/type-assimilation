package com.typeassimilation

import java.io.File
import com.typeassimilation.model.ModelPesistence
import com.typeassimilation.renderer.relational.RelationalRenderer
import com.typeassimilation.renderer.relational.RelationalModel
import com.typeassimilation.renderer.relational.RelationalRenderer.Config
import com.typeassimilation.renderer.relational.RelationalRenderer.PrimaryKeyPolicy

object App {
    def main(args: Array[String]): Unit = {
      val model = ModelPesistence.readDirectory(new File(args(0)))
      val logical = RelationalRenderer.render(model, Config(primaryKeyPolicy = PrimaryKeyPolicy.SurrogateKeyGeneration))
      println("******* LOGICAL *******")
      println(logical)
      println()
      println("******* CONCRETE LOGICAL *******")
      println(RelationalModel(logical))
      ModelPesistence.writeDirectory(model, new File(args(1)))
    }
}