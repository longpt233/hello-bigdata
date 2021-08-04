package com.company.bigdata.spark.api.graphx.rdd

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestGraphx {

  def main(args: Array[String]): Unit = {
    val conf= new SparkConf().setMaster("local[*]").setAppName("contex")
    val sc = new SparkContext(conf)

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Seq((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")), (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Seq(Edge(3L, 7L, "collab"),Edge(5L, 3L, "advisor"), Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    val defaultUser = ("John Doe", "Missing")

    val graph = Graph(users, relationships, defaultUser) // Graph[(String, String), String]

    println(graph.getClass.getName)

    val facts: RDD[String]= graph.triplets.map(e=> e.srcAttr._1+ " is " + e.attr + " of " +e.dstAttr._1)
    facts.foreach(println)
  }
}
