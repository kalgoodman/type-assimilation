package com.typeassimilation.implicits

import scala.xml.Elem
import scala.xml.Attribute
import scala.xml.MetaData
import scala.xml.Text
import EnhancedNodeSeq._

trait EnhancedElem {
  case class EnhancedElem(elem: Elem) {
    def attributeOption(attributeName: String) = (elem \ ("@" + attributeName)).headOption.map(_.text)
    def childElems = elem.child.flatMap(_ match { case e: Elem => Some(e); case _ => None })
    def childElemTextOption(elementName: String) = (elem \ elementName).headElemOption.map(_.text)
    def withAttributes(attributesToAdd: (String, Option[_])*) = elem.copy(attributes = attributesToAdd.foldRight(elem.attributes) {
      case ((newKey, newValueOption), previousAttributes) => newValueOption match {
        case Some(newValue) => Attribute(newKey, Seq(Text(newValue.toString)), previousAttributes)
        case None => previousAttributes        
      }
    })
  }
  implicit def elemToEnhanceElem(elem: Elem) = EnhancedElem(elem)
}
object EnhancedElem extends EnhancedElem

