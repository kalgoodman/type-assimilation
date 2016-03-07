package com.typeassimilation

import java.io.File
import com.typeassimilation.model.ModelPesistence
import com.typeassimilation.renderer.relational.RelationalRenderer
import com.typeassimilation.renderer.relational.RelationalModel

object App {
    def main(args: Array[String]): Unit = {
      val model = ModelPesistence.readDirectory(new File(args(0)))
      println(RelationalRenderer.render(model))
      ModelPesistence.writeDirectory(model, new File(args(1)))
    }
}