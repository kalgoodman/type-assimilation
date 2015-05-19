package com.typeassimilation

import java.io.File
import com.typeassimilation.model.ModelPesistence

object App {
    def main(args: Array[String]): Unit = {
      val model = ModelPesistence.readDirectory(new File(args(0)))
      ModelPesistence.writeDirectory(model, new File(args(1)))
    }
}