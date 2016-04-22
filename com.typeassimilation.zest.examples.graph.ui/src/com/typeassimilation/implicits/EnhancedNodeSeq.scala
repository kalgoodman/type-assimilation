package com.typeassimilation.implicits

import scala.xml.Elem
import scala.xml.NodeSeq
import scala.xml.Node

trait EnhancedNodeSeq {
  case class EnhancedNodeSeq(nodeSeq: NodeSeq) {
    private def toElemSeq(nodesAsSeq: Seq[Node]) = nodesAsSeq.flatMap(_ match {case e:Elem => Some(e); case _ => None}) 
    def toElemSeq:Seq[Elem] = toElemSeq(nodeSeq)
    def childElemSeq: Seq[Elem] = nodeSeq.flatMap(n => toElemSeq(n.child))
    def headElemOption = toElemSeq.headOption
    def headElem = toElemSeq.head
  }
  implicit def nodeSeqToEnhancedNodeSeq(nodeSeq: NodeSeq) = EnhancedNodeSeq(nodeSeq)
}
object EnhancedNodeSeq extends EnhancedNodeSeq