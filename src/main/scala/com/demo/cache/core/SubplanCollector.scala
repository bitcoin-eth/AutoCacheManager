package com.demo.cache.core

import com.demo.cache.model.{GlobalRepeatedCandidate, QueryUnit}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object SubplanCollector {

  private case class CollectedNode(
    fingerprint: String,
    nodeSize: Int,
    sourceUnitId: String,
    sourceObjectName: Option[String],
    rawText:String,
    queryText:String,
    plan: LogicalPlan
  )

  def collectRepeatedAcrossQueries(
    plans: Seq[(QueryUnit, LogicalPlan)],
    minOccurrences: Int,
    minNodeSize: Int
  ): Seq[GlobalRepeatedCandidate] = {

    val collected = plans.flatMap { case (unit, rootPlan) =>
      collectNodesFromSinglePlan(unit, rootPlan, minNodeSize)
    }

    val grouped = collected.groupBy(_.fingerprint)

    grouped.toSeq.flatMap { case (fp, nodes) =>
      if (nodes.size >= minOccurrences) {
        val representative = nodes.head
        Some(
          GlobalRepeatedCandidate(
            fingerprint = fp,
            occurrences = nodes.size,
            nodeSize = representative.nodeSize,
            sourceUnitIds = nodes.map(_.sourceUnitId).distinct.sorted,
            sourceObjectNames = nodes.flatMap(_.sourceObjectName).distinct.sorted,
            examplePlanText = representative.plan.treeString,
            sourceSqls=nodes
              .groupBy(n=>(n.sourceUnitId,n.sourceObjectName,n.rawText,n.queryText))
              .keys
              .toSeq
              .map{case (unitId,objectName,rawText,queryText)=>
                CandidateSourceSql(
                  unitId=unitId,
                  objectName=objectName,
                  rawText=rawText,
                  queryText=queryText
                )
              }
            .sortBy(_.unitId)
          )
        )
      } else {
        None
      }
    }.sortBy(c => (-c.nodeSize, -c.occurrences))
  }

  private def collectNodesFromSinglePlan(
    unit: QueryUnit,
    root: LogicalPlan,
    minNodeSize: Int
  ): Seq[CollectedNode] = {
    val buf = scala.collection.mutable.ArrayBuffer.empty[CollectedNode]

    root.foreach { node =>
      val size = countNodes(node)
      if (size >= minNodeSize && EligibilityChecker.isCacheable(node)) {
        buf += CollectedNode(
          fingerprint = PlanFingerprint.of(node),
          nodeSize = size,
          sourceUnitId = unit.unitId,
          sourceObjectName = unit.objectName,
          rawText = unit.rawText,
          queryText = unit.queryText,
          plan = node
        )
      }
    }

    buf.toSeq
  }

  private def countNodes(plan: LogicalPlan): Int = {
    var count = 0
    plan.foreach(_ => count += 1)
    count
  }
}
