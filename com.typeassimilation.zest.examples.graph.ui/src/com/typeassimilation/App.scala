package com.typeassimilation

import java.io.File
import com.typeassimilation.model.ModelPesistence
import com.typeassimilation.model.RelationalRenderer
import com.typeassimilation.model.RelationalRenderer.PrimaryKeyCreationPolicy

object App {
    def main(args: Array[String]): Unit = {
      val model = ModelPesistence.readDirectory(new File(args(0)))
      println(RelationalRenderer.render(model, RelationalRenderer.Config(primaryKeyCreationPolicy = RelationalRenderer.PrimaryKeyCreationPolicy.SurrogateKeyGeneration)))
      ModelPesistence.writeDirectory(model, new File(args(1)))
    }
}