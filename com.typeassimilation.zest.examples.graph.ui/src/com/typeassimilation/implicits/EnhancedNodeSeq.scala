package com.typeassimilation.implicits

import scala.xml.Elem
import scala.xml.NodeSeq

trait EnhancedNodeSeq {
  case class EnhancedNodeSeq(nodeSeq: NodeSeq) {
    def toElemSeq:Seq[Elem] = nodeSeq.flatMap(_ match {case e:Elem => Some(e); case _ => None}) 
    def headElemOption = toElemSeq.headOption
    def headElem = toElemSeq.head
  }
  implicit def nodeSeqToEnhancedNodeSeq(nodeSeq: NodeSeq) = EnhancedNodeSeq(nodeSeq)
}
object EnhancedNodeSeq extends EnhancedNodeSeq