package com.typeassimilation.implicits

import java.io.File
import com.typeassimilation.model.FilePath
import com.typeassimilation.model.FilePath.Implicits._

object EnhancedFile {

  class EnhancedFile(file: File) {
    
    def relativeTo(parentPath: String): FilePath.Absolute = relativeTo(new File(parentPath))
    
    def relativeTo(parent: File): FilePath.Absolute = {
      if (!file.getAbsolutePath.startsWith(parent.getAbsolutePath)) {
        throw new IllegalArgumentException(s"Parent path provided: '${parent.getAbsolutePath}' is not a parent of '${file.getAbsolutePath}'")
      }
      trimTrailingChars(file.getAbsolutePath.substring(parent.getAbsolutePath.length).replaceAll("\\\\", "/"), "/").toAbsolute
    }
  }
  
  implicit def fileToEnhancedFile(file: File) = new EnhancedFile(file)
  
  private def trimTrailingChars(input: String, char: String): String = {
    input.endsWith(char) match {
      case true => trimTrailingChars(input.dropRight(1), char)
      case false => input
    }
  }
}