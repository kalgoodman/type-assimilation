package com.typeassimilation.scala

import org.eclipse.gef4.zest.fx.ui.parts.ZestFxUiView
import com.google.inject.Guice
import com.google.inject.util.Modules
import org.eclipse.gef4.zest.fx.ZestFxModule
import org.eclipse.gef4.zest.fx.ui.ZestFxUiModule
import com.typeassimilation.model.Model
import scala.collection.mutable
import collection.JavaConversions._
import org.eclipse.gef4.graph.{ Edge, Node, Graph }
import org.eclipse.gef4.zest.fx.ZestProperties
import scalafx.Includes._
import scalafx.collections.ObservableBuffer
import scalafx.collections.ObservableBuffer.{ Add, Remove, Reorder, Update }
import com.typeassimilation.model.ModelPesistence
import java.io.File
import org.eclipse.gef4.layout.algorithms.SpringLayoutAlgorithm
import scalafx.collections.ObservableSet
import com.typeassimilation.model.DataType
import com.typeassimilation.model.Assimilation

class MultiEdge(initInboundEdges: Seq[Edge], initNode: Node, initOutboundEdges: Seq[Edge]) {
  val inboundEdges = mutable.ListBuffer(initInboundEdges: _*)
  var node = initNode
  val outboundEdges = mutable.ListBuffer(initOutboundEdges: _*)
  def edges = inboundEdges.toSeq ++ outboundEdges
}

class ScalalalaActualView extends ZestFxUiView(Guice.createInjector(Modules.`override`(new ZestFxModule()).`with`(new ZestFxUiModule()))) {
  val model = ModelPesistence.readDirectory(new File("C:/eclipse-workspaces/luna-experiment/type-assimilation-testing"))
  val typeNameToNodeMap = mutable.Map.empty[String, Node]
  def toNode(dt: DataType) = {
    val node = new Node
    ZestProperties.setLabel(node, dt.name)
    node
  }
  def toMultiEdge(a: Assimilation): MultiEdge = {
    val node = new Node
    ???
  }
  def name(n: Node) = n.getAttrs.apply(ZestProperties.ELEMENT_LABEL).toString 
  val assimilationNameToEdgeMap = mutable.Map.empty[String, MultiEdge]
  val graph = {
    val nodes = model.dataTypes.map(toNode(_))
    typeNameToNodeMap ++= nodes.map(n => name(n) -> n).toMap
    val edges = model.assimilations.map(toMultiEdge(_))
    assimilationNameToEdgeMap ++= edges.map(me => name(me.node) -> me)
    val graphAttributes = Map(
      ZestProperties.GRAPH_TYPE -> ZestProperties.GRAPH_TYPE_DIRECTED,
      ZestProperties.GRAPH_LAYOUT -> new SpringLayoutAlgorithm)
    new Graph(graphAttributes, nodes.toSeq ++ edges.map(_.node), edges.flatMap(_.edges).toSeq)
  }

  setGraph(graph)
}